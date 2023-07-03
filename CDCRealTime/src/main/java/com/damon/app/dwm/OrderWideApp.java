package com.damon.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.damon.app.function.DimAsyncFunction;
import com.damon.bean.OrderDetail;
import com.damon.bean.OrderInfo;
import com.damon.bean.OrderWide;
import com.damon.utils.MyKafkaUtil;
import com.damon.utils.TimeUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.concurrent.TimeUnit;

import static com.damon.utils.EnvUtil.getEnv;
import static com.damon.utils.PropUtil.getDimInfoProp;
import static com.damon.utils.PropUtil.getKafkaProp;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();

        String orderInfoSourceTopic = getKafkaProp("dwm_order_wide.info_topic");
        String orderDetailSourceTopic = getKafkaProp("dwm_order_wide.detail_topic");
        String orderWideSinkTopic = getKafkaProp("dwm_order_wide.sink_topic");
        String groupId = getKafkaProp("dwm_order_wide.group_id");


        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] dataTimeArr = create_time.split(" ");       // yyyy-MM-dd HH:mm:ss
                    orderInfo.setCreate_date(dataTimeArr[0]);
                    orderInfo.setCreate_hour(dataTimeArr[1].split(":")[0]);

                    orderInfo.setCreate_ts(TimeUtil.getTime(create_time));
                    return orderInfo;
                })
                // monotonous timestamp 单调递增的时间戳
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<OrderInfo>) (orderInfo, l) -> orderInfo.getCreate_ts())
                );
//        .assignTimestampsAndWatermarks(
//                WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
//                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
//                            @Override
//                            public long extractTimestamp(OrderInfo orderInfo, long l) {
//                                return orderInfo.getCreate_ts();
//                            }
//                        })
//        )

        // TODO: 2023/7/3 有没有好的方法将这几段代码给重写了，减少代码量
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();

                    orderDetail.setCreate_ts(TimeUtil.getTime(create_time));
                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<OrderDetail>) (OrderDetail orderDetail, long l) -> orderDetail.getCreate_ts())
                );

        SingleOutputStreamOperator<OrderWide> orderWideWithoutDimInfoDS = orderInfoDS
                .keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        orderWideWithoutDimInfoDS.print("orderWideWithoutDimInfoDS>>>>>>>>>>>>>>>>");


        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithoutDimInfoDS,
                new DimAsyncFunction<OrderWide>(getDimInfoProp("dim.province")) {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS
        );


        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>(getDimInfoProp("dim.sku")) {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS
        );


        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>(getDimInfoProp("dim.spu")) {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>(getDimInfoProp("dim.trademark")) {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS,
                new DimAsyncFunction<OrderWide>(getDimInfoProp("dim.category3")) {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setCategory3_name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS
        );

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>>>>>>");

        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        env.execute("OrderWideApp");
    }


}
