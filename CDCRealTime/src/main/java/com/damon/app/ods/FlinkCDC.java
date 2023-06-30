package com.damon.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.damon.app.function.CustomerDeserialization;
import com.damon.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import static com.damon.utils.EnvUtil.getEnv;
import static com.damon.utils.PropUtil.getKafkaProp;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = getEnv();

        // 通过FlinkCDC勾线 SourceFunction 并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        String sinkTopic = getKafkaProp("ods.sink_topic");
        streamSource.print(">>>>>>>>>>>");
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("FlinkCDC");
    }
}
