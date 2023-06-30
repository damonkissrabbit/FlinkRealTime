package com.damon.app.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.damon.utils.EnvUtil.getEnv;
import static com.damon.utils.PropUtil.getKafkaProp;

public class BaseLogApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = getEnv();

        String sourceTopic = getKafkaProp("dwd_log.source_topic");
        String groupId = getKafkaProp("dwd_log.group_id");
    }
}
