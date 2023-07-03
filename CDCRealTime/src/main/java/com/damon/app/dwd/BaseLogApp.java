package com.damon.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.damon.utils.MyKafkaUtil;
import io.debezium.data.Json;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static com.damon.utils.EnvUtil.getEnv;
import static com.damon.utils.PropUtil.getKafkaProp;

/**
 * 处理来自spring的数据，对不同类型的数据进行分流。
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();

        // ods_base_log 该主题的数据来源于业务系统 spring 发过来的数据
        String sourceTopic = getKafkaProp("dwd_log.source_topic");
        String groupId = getKafkaProp("dwd_log.group_id");

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 将每行数据转换为JSON对象
        // 侧输出流，脏数据
        OutputTag<String> outputTag = new OutputTag<>("Dirty");
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        });

        jsonDS.getSideOutput(outputTag).print("--------------Dirty-------------");

        /*
          新老用户校验 状态编程
          根据mid字段来进行分类 keyBy
          valueState中保存每次数据过来的 is_New 字段
          保存每个 mid 的首次访问日期，每条进入该算子的访问记录，
          都会把每条mid对应的首次访问时间读取出来
          只要首次访问时间不为空，则认为该访问时老访客，否则是新访客
          如果是新访客切没有访问记录的话， 会写入首次访问时间
          ods_base_log
          {
              "common":{
                  "ar":"440000",
                  "ba":"vivo",
                  "ch":"oppo",
                  "is_new":"1",
                  "md":"vivo iqoo3",
                  "mid":"mid_14",
                  "os":"Android 11.0",
                  "uid":"1",
                  "vc":"v2.1.134"
              },
              "start":{
                  "entry":"icon",
                  "loading_time":11704,
                  "open_ad_id":14,
                  "open_ad_ms":1010,
                  "open_ad_skip_ms":1001
              },
              "ts":1660483545000
          }
         */
        // TODO: 2023/7/1 这里留下一个问题，这里的状态保存多久？要不要设置过期时间？
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> firstVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {

                        String isNew = value.getJSONObject("common").getString("is_new");

                        // 只有两种状态 0, 1
                        // 如果是 0 的话，该用户是老用户，不用判断，直接输出就行。
                        if ("1".equals(isNew)) {
                            // 取出 firstVisitState 里面的数据
                            String firstDate = firstVisitState.value();

                            if (firstDate != null) {
                                // 已经是老用户了，但是来的数据里面的 is_new 字段是 1，要将该字段修改为 0
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                // 是新用户，将 firstVisitState 里面的状态修改为 1
                                firstVisitState.update("1");
                            }
                        }

                        return value;
                    }
                });

        // 分流 侧输出流 页面：主流  启动：侧输出流 曝光：侧输出流
        OutputTag<String> startTag = new OutputTag<>("start");
        OutputTag<String> displayTag = new OutputTag<>("display");

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                String start = value.getString("start");

                if (start != null && start.length() > 0) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    out.collect(value.toJSONString());

                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print(">>>>>>>>>>>>>>>>page");
        startDS.print(">>>>>>>>>>>>>>>start");
        displayDS.print(">>>>>>>>>>>>>display");

        startDS.addSink(MyKafkaUtil.getKafkaProducer(getKafkaProp("dwd_log.start_sink")));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer(getKafkaProp("dwd_log.page_sink")));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer(getKafkaProp("dwd_log.display_sink")));

        env.execute("DwdBaseLog");
    }
}
