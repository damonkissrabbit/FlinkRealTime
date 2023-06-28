package com.damon.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class PropUtil {

    public static String getKafkaProp(String name) throws IOException {
        ParameterTool tool = ParameterTool.fromPropertiesFile("CDCRealTime/src/main/resources/kafka_topic.properties");
        return tool.getRequired(name);
    }

    public static String getMysqlProp(String name) throws IOException {
        ParameterTool tool = ParameterTool.fromPropertiesFile("CDCRealTime/src/main/resources/mysql.properties");
        return tool.getRequired(name);
    }
}
