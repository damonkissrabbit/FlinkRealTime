package com.damon.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static final String brokers = "localhost:9092";
    private static final String default_topic = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<>(
                brokers,
                topic,
                new SimpleStringSchema()
        );
    }

    // <T> 声明此方法持有一个类型 T，也可以理解为申明此方法为泛型方法
    // 定义一个泛型方法时，必须再返回之前面要加上一个 <T>，来声明这是一个泛型方法，持有一个泛型 T，然后才可以用反向T作为方法的返回值
    // 定义了一个泛型类型为 T 的返回值 FlinkKafkaProducer<T>.
    // 该方法接受一个类型为 KafkaSerializationSchema<T> 的参数 kafkaSerializationSchema
    // 这个方法提供了更加灵活的方式，可以根据不同的数据类型和序列化需求自定义序列化逻辑。
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return new FlinkKafkaProducer<>(
                default_topic,
                kafkaSerializationSchema,
                prop,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

    public static String getKafkaDDL(String topic, String groupId) {
        return " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset'  ";
    }
}
