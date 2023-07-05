package com.damon.bean;
import lombok.Data;
import lombok.AllArgsConstructor;


@Data
@AllArgsConstructor
public class VisitorStats {
    private String startTime;
    private String endTime;
    private String version;
    private String channel;
    private String ares;
    private String is_new;
    private Long uvCount = 0L;
    private Long pvCount = 0L;
    private Long svCount = 0L;
    private Long ujCount = 0L;
    private Long durationSum = 0L;
    private Long ts;

}
