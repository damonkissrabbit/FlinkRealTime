package com.damon.app.dwm;

import com.alibaba.fastjson.JSON;
import com.damon.bean.OrderWide;
import com.damon.utils.TimeUtil;
import com.damon.bean.PaymentInfo;
import com.damon.bean.PaymentWide;
import com.damon.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;

import static com.damon.utils.EnvUtil.getEnv;
import static com.damon.utils.PropUtil.getKafkaProp;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();

        String paymentInfoSourceTopic = getKafkaProp("dwm_payment_wide.payment_source_topic");
        String orderWideSourceTopic = getKafkaProp("dwm_payment_wide.orderWide_source_topic");
        String paymentWideSinkTopic = getKafkaProp("dwm_payment_wide.sink_topic");
        String groupId = getKafkaProp("dwm_payment_wide.group_id");

        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<OrderWide>) (orderWide, recordTimestamp) -> {
                                    try {
                                        return TimeUtil.getTime(orderWide.getCreate_time());
                                    } catch (ParseException e) {
                                        e.printStackTrace();
                                    }
                                    return recordTimestamp;
                                })
                );

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(data -> JSON.parseObject(data, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<PaymentInfo>) (paymentInfo, recordTimestamp) -> {
                                    try {
                                        return TimeUtil.getTime(paymentInfo.getCreate_time());
                                    } catch (ParseException e) {
                                        e.printStackTrace();
                                    }
                                    return recordTimestamp;
                                })
                );

        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS
                .keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.minutes(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        paymentWideDS.print("paymentWideDS>>>>>>>");
        paymentWideDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        env.execute("PaymentWideApp");
    }
}
