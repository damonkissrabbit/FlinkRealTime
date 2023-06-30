package com.damon.app;

import com.alibaba.fastjson.JSONObject;
import com.damon.bean.TableProcess;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class demo {
    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {

        // class com.damon.app.demo
        JSONObject jsonObject = JSONObject.class.newInstance();

        jsonObject.put("name", "yuyu");
        System.out.println(jsonObject);

        TableProcess tableProcess = TableProcess.class.newInstance();
        tableProcess.setOperateType("aaa");
        System.out.println(tableProcess.toString());

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

//        ParameterTool tool = ParameterTool.fromPropertiesFile("CDCRealTime/src/main/resources/kafka_topic.properties");
//        String sinkTopic = tool.get("ods.sink_topic");
//        System.out.println(sinkTopic);

//        JSONObject jsonObject = new JSONObject();
//
//// 获取键值对集合
//        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
//
//// 遍历键值对集合
//        for (Map.Entry<String, Object> entry : entries) {
//            String key = entry.getKey();
//            Object value = entry.getValue();
//
//            // 对键值对进行操作
//            System.out.println("Key: " + key);
//            System.out.println("Value: " + value);
//        }


    }
}
