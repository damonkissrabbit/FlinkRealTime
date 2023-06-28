package com.damon.app;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;

public class demo {
    public static void main(String[] args) throws IOException {
//        Schema schema = SchemaBuilder.struct()
//                .field("name", Schema.STRING_SCHEMA)
//                .field("age", Schema.INT32_SCHEMA)
//                .build();
//
//        Struct struct = new Struct(schema);
//
//        struct.put("name", "join");
//        struct.put("age", 30);
//
//        String name = struct.getString("name");
//        int age = struct.getInt32("age");
//
//        for (Field field: schema.fields()) {
//            String fieldName = field.name();
//            Object fieldValue = struct.get(fieldName);
//            System.out.println(fieldName + ": " + fieldValue);
//        }

        ParameterTool tool = ParameterTool.fromPropertiesFile("CDCRealTime/src/main/resources/kafka_topic.properties");
        String sinkTopic = tool.get("ods.sink_topic");
        System.out.println(sinkTopic);

    }
}
