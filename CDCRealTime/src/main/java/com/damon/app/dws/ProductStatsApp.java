package com.damon.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.damon.app.function.DimAsyncFunction;
import com.damon.bean.OrderWide;
import com.damon.bean.PaymentWide;
import com.damon.bean.ProductStats;
import com.damon.common.GmallConstant;
import com.damon.utils.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getEnv();

        String pageViewSourceTopic = PropUtil.getKafkaProp("dws_product_stats.page_view_source_topic");
        String orderWideSourceTopic = PropUtil.getKafkaProp("dws_product_stats.order_wide_source_topic");
        String paymentWideSourceTopic = PropUtil.getKafkaProp("dws_product_stats.payment_wide_source_topic");
        String cartInfoSourceTopic = PropUtil.getKafkaProp("dws_product_stats.cart_info_source_topic");
        String favorInfoSourceTopic = PropUtil.getKafkaProp("dws_product_stats.favor_info_source_topic");
        String refundInfoSourceTopic = PropUtil.getKafkaProp("dws_product_stats.refund_info_source_topic");
        String commentInfoSourceTopic = PropUtil.getKafkaProp("dws_product_stats.comment_info_source_topic");
        String groupId = PropUtil.getKafkaProp("dws_product_stats.group_id");

        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> orderDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

        SingleOutputStreamOperator<ProductStats> productStatWithClickAndDisplayDs = pvDS.flatMap((FlatMapFunction<String, ProductStats>) (value, out) -> {
            JSONObject jsonObject = JSON.parseObject(value);

            JSONObject page = jsonObject.getJSONObject("page");
            String pageId = page.getString("page_id");

            Long ts = jsonObject.getLong("ts");

            if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                out.collect(
                        ProductStats
                                .builder()
                                .sku_id(page.getLong("item"))
                                .click_ct(1L)
                                .ts(ts)
                                .build()
                );
            }

            JSONArray displays = jsonObject.getJSONArray("displays");
            if (displays != null && displays.size() > 0) {
                for (int i = 0; i < displays.size(); i++) {
                    JSONObject display = displays.getJSONObject(i);

                    if ("sku_id".equals(display.getString("item_type"))) {
                        out.collect(
                                ProductStats
                                        .builder()
                                        .sku_id(display.getLong("item"))
                                        .display_ct(1L)
                                        .ts(ts)
                                        .build()
                        );
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });


        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = payDS.map(line -> {

            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            String appraise = jsonObject.getString("appraise");
            long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> reduceDS = productStatWithClickAndDisplayDs
                .union(
                        productStatsWithFavorDS,
                        productStatsWithCartDS,
                        productStatsWithOrderDS,
                        productStatsWithPaymentDS,
                        productStatsWithRefundDS,
                        productStatsWithCommentDS
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<ProductStats>) (element, recordTimestamp) -> element.getTs()
                                )
                )
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        (ReduceFunction<ProductStats>) (value1, value2) -> {
                            value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                            value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                            value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                            value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                            value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                            value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                            //value1.setOrder_ct(value1.getOrderIdSet().size() + 0L);
                            value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                            value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));

                            value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                            //value1.setRefund_order_ct(value1.getRefundOrderIdSet().size() + 0L);
                            value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));

                            value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                            //value1.setPaid_order_ct(value1.getPaidOrderIdSet().size() + 0L);

                            value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                            value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());
                            return value1;
                        },
                        (WindowFunction<ProductStats, ProductStats, Long, TimeWindow>) (key, window, input, out) -> {
                            ProductStats productStats = input.iterator().next();

                            productStats.setStartTime(DateTimeUtil.toYMDhs(new Date(window.getStart())));
                            productStats.setEndTime(DateTimeUtil.toYMDhs(new Date(window.getEnd())));

                            productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                            productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                            productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                            out.collect(productStats);
                        }
                );

        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                        productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getSpu_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 7.3 关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(
                productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                        productStats.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getCategory3_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 7.4 关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(
                productStatsWithCategory3DS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                        productStats.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getTm_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        productStatsWithTmDS.print("productStatsWithTmDS>>>>>>>>>>>>>>");
        productStatsWithTmDS.addSink(ClickHouseUtil.getSink(
                "insert into table product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        env.execute("ProductStatsApp");
    }
}
