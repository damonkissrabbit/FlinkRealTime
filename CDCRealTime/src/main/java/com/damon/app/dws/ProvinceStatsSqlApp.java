package com.damon.app.dws;


import org.apache.flink.table.api.Table;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.damon.bean.ProvinceStats;

import com.damon.utils.EnvUtil;
import com.damon.utils.PropUtil;
import com.damon.utils.MyKafkaUtil;
import com.damon.utils.ClickHouseUtil;


public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getEnv();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String orderWideTopic = PropUtil.getKafkaProp("dws_province_stats.order_wide_topic");
        String groupId = PropUtil.getKafkaProp("dws_province_stats.group_id");

        tableEnv.executeSql(
                "create table order_wide(\n" +
                        "\t`province_id` BIGINT,\n" +
                        "\t`province_name` STRING,\n" +
                        "\t`province_area_code` STRING,\n" +
                        "\t`province_iso_code` STRING,\n" +
                        "\t`province_3166_2` STRING,\n" +
                        "\t`order_id` BIGINT,\n" +
                        "\t`split_total_amount` DECIMAL,\n" +
                        "\t`create_time` STRING,\n" +
                        "\t`rt` as TO_TIMESTAMP(create_time),\n" +
                        "\tWATERMARK FOR rt AS rt -INTERVAL '1' SECOND \n" +
                        ") with ("
                        + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        Table table = tableEnv.sqlQuery(
                "SELECT\n" +
                        "\tDATE_FORMAT(TUMBLE_START(rt, INTERVAL, '10' SECOND), 'yyyy-MM-mm HH:mm:ss') startTime,\n" +
                        "\tDATE_FORMAT(TUMBLE_END(rt, INTERVAL, '10' SECOND), 'yyyy-MM-mm HH:mm:ss') endTime,\n" +
                        "\tprovince_id,\n" +
                        "\tprovince_name,\n" +
                        "\tprovince_area_code,\n" +
                        "\tprovince_iso_code,\n" +
                        "\tprovince_3166_2,\n" +
                        "\tCOUNT(DISTINCT order_id) order_count,\n" +
                        "\tSUM(split_total_amount) order_amount,\n" +
                        "\tUNIX_TIMESTAMP() * 1000 ts,\n" +
                        "FROM\n" +
                        "\torder_wide\n" +
                        "GROUP BY\n" +
                        "\tprovince_id, province_name, province_iso_code, province_area_code, province_3166_2,\n" +
                        "\tTUMBLE(rt, INTERVAL '10' SECOND)"
        );

        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        provinceStatsDataStream.print("provinceStatsDataStream>>>>>>>>>");
        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats values (?,?,?,?,?,?,?,?,?,?)"));

        env.execute("ProvinceStatsSqlApp");
    }
}
