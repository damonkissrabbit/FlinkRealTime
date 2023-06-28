package com.damon.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.damon.app.function.CustomerDeserialization;
import com.damon.app.function.TableProcessFunction;
import com.damon.bean.TableProcess;
import com.damon.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

import static com.damon.utils.EnvUtil.getEnv;
import static com.damon.utils.PropUtil.getKafkaProp;
import static com.damon.utils.PropUtil.getMysqlProp;

public class BaseDBApp {
    public static void main(String[] args) throws IOException {
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
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDs.broadcast(mapStateDescriptor);

        // 连接主流和广播流，主流 kafka 从 ods 输出的 kafka topic "ods_base_db" 获取的数据
        // 广播流来自 mysql 使用 flink cdc 监控 gmall_flink.table_process 表获得的
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        OutputTag<JSONObject> hbaseTag = new OutputTag<>("hbase-tag");

        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

    }
}
