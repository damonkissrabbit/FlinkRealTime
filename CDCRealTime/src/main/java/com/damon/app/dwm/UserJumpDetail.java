package com.damon.app.dwm;

import java.util.Map;
import java.util.List;
import java.time.Duration;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.cep.CEP;
import org.apache.flink.util.OutputTag;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.damon.utils.EnvUtil;
import com.damon.utils.PropUtil;
import com.damon.utils.MyKafkaUtil;

public class UserJumpDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getEnv();

        String sourceTopic = PropUtil.getKafkaProp("dwm_user_jump_detail.source_topic");
        String sinkTopic = PropUtil.getKafkaProp("dwm_user_jump_detail.sink_topic");
        String groupId = PropUtil.getKafkaProp("dwm_user_jump_detail.group_id");

        KeyedStream<JSONObject, String> dataStream = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTime) -> element.getLong("ts"))
                )
                .keyBy(data -> data.getJSONObject("page").getString("mid"));

        // 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                .within(Time.seconds(10));

//        Pattern.<JSONObject>begin("start")
//                .where(new SimpleCondition<JSONObject>() {
//                    @Override
//                    public boolean filter(JSONObject value) throws Exception {
//                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
//                        return lastPageId == null || lastPageId.length() <= 0;
//                    }
//                })
//                .times(2)
//                .consecutive()    // 指定严格临近为 next()
//                .within(Time.seconds(10));

        OutputTag<JSONObject> timeOutTag = new OutputTag<>("timeout");
        SingleOutputStreamOperator<JSONObject> selectDS = CEP.pattern(dataStream, pattern)
                .select(
                        timeOutTag,
                        (PatternTimeoutFunction<JSONObject, JSONObject>) (Map<String, List<JSONObject>> map, long var2) -> map.get("start").get(0),
                        (PatternSelectFunction<JSONObject, JSONObject>) map -> map.get("start").get(0)
                );

        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        unionDS.print("unionDS>>>>>>>>>>>>>>>>");

        unionDS
                .map(JSONArray::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("UserJumpDetailApp");
    }
}
