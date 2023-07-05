package com.damon.app.dws;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import scala.Tuple4;

import java.util.Date;
import java.time.Duration;

import com.damon.utils.*;
import com.damon.bean.VisitorStats;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getEnv();

        String uniqueVisitorSourceTopic = PropUtil.getKafkaProp("dws_visitor_stats.unique_visitor_source_topic");
        String userJumpDetailSourceTopic = PropUtil.getKafkaProp("dws_visitor_stats.user_jump_detail_source_topic");
        String pageViewSourceTopic = PropUtil.getKafkaProp("dws_visitor_stats.page_view_source_topic");
        String groupId = PropUtil.getKafkaProp("dws_visitor_stats.group_id");

        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitorSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        //3.1 处理UV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });


        //3.2 处理UJ数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            //提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });

        //3.3 处理PV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            String last_page_id = page.getString("last_page_id");

            long sv = 0L;

            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> resultDS = visitorStatsWithUvDS
                .union(
                        visitorStatsWithUjDS,
                        visitorStatsWithPvDS
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<VisitorStats>) (element, l) -> element.getTs()
                                )
                )
                .keyBy(
                        (KeySelector<VisitorStats, Tuple4<String, String, String, String>>) visitorStats -> new Tuple4<>(
                                visitorStats.getAres(),
                                visitorStats.getChannel(),
                                visitorStats.getIs_new(),
                                visitorStats.getVersion()
                        )
                )
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        (ReduceFunction<VisitorStats>) (value1, value2) -> {
                            value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                            value1.setPvCount(value1.getPvCount() + value2.getPvCount());
                            value1.setSvCount(value1.getSvCount() + value2.getSvCount());
                            value1.setUjCount(value1.getUjCount() + value2.getUjCount());
                            value1.setDurationSum(value1.getDurationSum() + value2.getDurationSum());

                            return value1;
                        },
                        (WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>)
                                (key, window, input, out) -> {
                                    long start = window.getStart();
                                    long end = window.getEnd();

                                    VisitorStats visitorStats = input.iterator().next();

                                    visitorStats.setStartTime(DateTimeUtil.toYMDhs(new Date(start)));
                                    visitorStats.setEndTime(DateTimeUtil.toYMDhs(new Date(end)));

                                    out.collect(visitorStats);
                                }
                );

        resultDS.print("resultDS>>>>>>>>>>>>>");
        resultDS.addSink(ClickHouseUtil.getSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute("VisitorStatsApp");
    }
}
