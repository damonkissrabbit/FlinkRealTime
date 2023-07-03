package com.damon.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.damon.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

import static com.damon.utils.EnvUtil.getEnv;
import static com.damon.utils.PropUtil.getKafkaProp;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();

        String sourceTopic = getKafkaProp("dwm_unique_visit.source_topic");
        String sinkTopic = getKafkaProp("dwm_unique_visit.sink_topic");
        String groupId = getKafkaProp("dwm_unique_visit.group_id");

        SingleOutputStreamOperator<JSONObject> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                .map(JSON::parseObject)
                .keyBy(data -> data.getJSONObject("common").getString("mid"))
                .filter(new RichFilterFunction<JSONObject>() {

                    private ValueState<String> dateState;
                    private SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                        StateTtlConfig stateTtlConfig = new StateTtlConfig
                                .Builder(Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                        dateState = getRuntimeContext().getState(valueStateDescriptor);

                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {

                        // 取出上一条页面信息
                        // 如果这一条数据中没有last_page_id这个字段，那么得到的就是null
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");

                        // 判断第一条页面是否为null
                        if (lastPageId == null || lastPageId.length() <= 0) {
                            // 取出状态数据
                            String lastDate = dateState.value();
                            // 取出现在的日期
                            String currentDate = simpleDateFormat.format(value.getLong("ts"));

                            // 判断两个日期是否相同，两个日期不相同，更新状态
                            if (!currentDate.equals(lastDate)) {
                                dateState.update(currentDate);
                                return true;
                            }
                        }
                        return false;
                    }
                });
        uvDS.print("uvDS>>>>>>>>>>>>>>>>>>");
        uvDS
                .map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        env.execute("UniqueVisitApp");
    }
}
