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

        String jsonString = "{\n" +
                "\t\"common\": {\n" +
                "\t\t\"ar\": \"440000\",\n" +
                "\t\t\"ba\": \"vivo\",\n" +
                "\t\t\"ch\": \"oppo\",\n" +
                "\t\t\"is_new\": \"1\",\n" +
                "\t\t\"md\": \"vivo iqoo3\",\n" +
                "\t\t\"mid\": \"mid_14\",\n" +
                "\t\t\"os\": \"Android 11.0\",\n" +
                "\t\t\"uid\": \"1\",\n" +
                "\t\t\"vc\": \"v2.1.134\"\n" +
                "\t},\n" +
                "\t\"start\": {\n" +
                "\t\t\"entry\": \"icon\",\n" +
                "\t\t\"loading_time\": 11704,\n" +
                "\t\t\"open_ad_id\": 14,\n" +
                "\t\t\"open_ad_ms\": 1010,\n" +
                "\t\t\"open_ad_skip_ms\": 1001\n" +
                "\t},\n" +
                "\t\"ts\": 1660483545000\n" +
                "}";

        JSONObject jsonObj = JSONObject.parseObject(jsonString);
        System.out.println(jsonObj.getString("start"));


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
