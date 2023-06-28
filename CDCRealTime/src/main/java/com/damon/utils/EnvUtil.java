package com.damon.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvUtil {
    public static StreamExecutionEnvironment getEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
