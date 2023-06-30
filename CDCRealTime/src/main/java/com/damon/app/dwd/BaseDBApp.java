package com.damon.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.damon.app.function.CustomerDeserialization;
import com.damon.app.function.DinSinkFunction;
import com.damon.app.function.TableProcessFunction;
import com.damon.bean.TableProcess;
import com.damon.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import static com.damon.utils.EnvUtil.getEnv;
import static com.damon.utils.PropUtil.getKafkaProp;
import static com.damon.utils.PropUtil.getMysqlProp;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();

        String sourceTopic = getKafkaProp("dwd_db.source_topic");
        String groupId = getKafkaProp("dwd_db.group_id");

        // 从 kafka 读出主流数据 kafka中 的数据是从 flink cdc 服务发送到 ods_base_db 里面的
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDs.map(JSON::parseObject)
                .filter((FilterFunction<JSONObject>) data -> {
                    String type = data.getString("type");
                    return !"delete".equals(type);
                });


        // 使用 flink cdc 读取配置信息表并处理成广播流
        // 这里的 flink cdc 只监控 table_process 表，用来创建表，然后在 processElementBroadcast 里面把
        // sourceTable + 操作类型 存入状态里面
        // 在上面 kafka 流数据来以后（也就是mysql数据库里面数据变更之后）
        // 在 processElement 里面先判断能不能对该表进行操作，然后再对数据进行处理 分流(分到 hbase 还是 kafka 里面)
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(getMysqlProp("hostname"))
                .port(Integer.parseInt(getMysqlProp("port")))
                .username(getMysqlProp("username"))
                .password(getMysqlProp("password"))
                .databaseList(getMysqlProp("databaseList"))
                .tableList(getMysqlProp("dwd_db.tableList"))
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDs = env.addSource(sourceFunction);

        // 广播数据的格式
        // 广播流可以通过查询配置文件，广播到某个operator的所有并发实例中，然后与另一条数据连接进行计算
        // 创建 MapStateDescriptor 对象，用来描述广播状态的名称和类型
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDs.broadcast(mapStateDescriptor);

        // 连接主流和广播流，主流 kafka 从 ods 输出的 kafka topic "ods_base_db" 获取的数据
        // 广播流来自 mysql 使用 flink cdc 监控 gmall_flink.table_process 表获得的
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        // 分流 处理速度 广播流数据，主流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<>("hbase-tag");

        // 传入的是 侧输出流 hbaseTag 和 广播数据的格式 mapStateDescriptor, 可以通过对应的 mapStateDescriptor 获取到相应的广播流数据
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        kafka.print("kafka>>>>>>>>>>>>>>>>");
        hbase.print("hbase>>>>>>>>>>>>>>>>");

        hbase.addSink(new DinSinkFunction());
        // (KafkaSerializationSchema<JSONObject>) 是一个类型转换表达式，将匿名累不累转换为 KafkaSerializationSchema<JSONObject> 类型
        // 匿名内部类实现了 kafkaSerializationSchema 接口，并定义了 serialize 方法来讲 JSONObject 类型的数据序列化为 ProducerRecord
        // (element, timestamp) -> new ProducerRecord<>(...)  这是匿名内部类实现的 serialize 方法的具体实现，它接受两个参数
        // element 和 timestamp, 并返回一个 ProducerRecord 对象，在这里，根据 element 中的数据创建一个新的 ProducerRecord 对象
        kafka.addSink(MyKafkaUtil.getKafkaProducer((KafkaSerializationSchema<JSONObject>) (element, timestamp)
            -> new ProducerRecord<>(
                    element.getString("sinkTable"),
                    element.getString("after").getBytes()
                )
        ));
        env.execute();
    }
}
