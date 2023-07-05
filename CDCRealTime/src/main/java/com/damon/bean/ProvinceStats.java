package com.damon.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 地区统计宽表实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProvinceStats {
    private String startTime;
    private String endTime;
    private Long province_id;
    private String province_name;
    private String province_area_code;
    private String province_iso_code;
    private String province_3166_2_code;
    private BigDecimal order_amount;
    private Long order_count;
    private Long ts;

    public ProvinceStats(OrderWide orderWide) {
        province_id = orderWide.getProvince_id();
        order_amount = orderWide.getSplit_total_amount();
        province_name = orderWide.getProvince_name();
        province_area_code = orderWide.getProvince_area_code();
        province_iso_code = orderWide.getProvince_iso_code();
        province_3166_2_code = orderWide.getProvince_3166_2_code();

        order_count = 1L;
        ts = new Date().getTime();
    }

}
