package com.damon.bean;

import lombok.Data;
import lombok.Builder;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@Data
@Builder
// @Builder 注解，可以使用构造者方式构建对象，给属性赋值
// @Builder.Default 在使用构造者方式给属性赋值的时候，属性的初始值会丢失
public class ProductStats {

    String startTime;
    String endTime;
    Long sku_id;
    String sku_name;
    BigDecimal sku_price;
    Long spu_id;
    String spu_name;
    Long tm_id;
    String tm_name;
    Long category3_id;
    String category3_name;

    @Builder.Default
    Long display_ct = 0L;

    @Builder.Default
    Long click_ct = 0L;  //点击数

    @Builder.Default
    Long favor_ct = 0L; //收藏数

    @Builder.Default
    Long cart_ct = 0L;  //添加购物车数

    @Builder.Default
    Long order_sku_num = 0L; //下单商品个数

    @Builder.Default   //下单商品金额
    BigDecimal order_amount = BigDecimal.ZERO;

    @Builder.Default
    Long order_ct = 0L; //订单数

    @Builder.Default   //支付金额
    BigDecimal payment_amount = BigDecimal.ZERO;

    @Builder.Default
    Long paid_order_ct = 0L;  //支付订单数

    @Builder.Default
    Long refund_order_ct = 0L; //退款订单数

    @Builder.Default
    BigDecimal refund_amount = BigDecimal.ZERO;

    @Builder.Default
    Long comment_ct = 0L;//评论订单数

    @Builder.Default
    Long good_comment_ct = 0L; //好评订单数

    @Builder.Default
    @TransientSink
    Set orderIdSet = new HashSet();  //用于统计订单数

    @Builder.Default
    @TransientSink
    Set paidOrderIdSet = new HashSet(); //用于统计支付订单数

    @Builder.Default
    @TransientSink
    Set refundOrderIdSet = new HashSet();//用于退款支付订单数

    Long ts; //统计时间戳
}
