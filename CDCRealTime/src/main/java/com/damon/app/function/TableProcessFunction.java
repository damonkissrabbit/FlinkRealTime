package com.damon.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.damon.bean.TableProcess;
import com.damon.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


/**
 * processBroadcastElement 方法的执行先于 processElement 方法，在处理主输入流的元素之前
 * 需要先处理广播流的元素，以确保输入流可以使用最新的广播状态数据进行处理
 */

/**
 * 怎么确定一条数据应该进入kafka还是hbase的？
 * 广播流监控的是table_process这张表，这张表里面的source_table字段对应这数据库里面所有表的信息（处理table_process）这张表
 * 广播流以 table_process 里面的 source_table + "_" + operate_type 作为 key，tableProcess (也就是table_process)中的一行数据
 * 作为value，来进行广播。这些操作都是在 processBroadcastElement 这个方法里面完成
 * 后面在processElement里面通过对应的key来取出相关的广播流数据，能取到是因为进入processElement里面的value是进过Flink cdc那边处理过的
 * 在进过flink cdc处理完之后的json中，有tableName和type字段分别对应了变更数据的表名和操作类型（insert update delete）
 * 如果操作类型是delete的话 hbase默认不做处理（就是多了一条维度信息，其余的不会有影响）
 *
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private final OutputTag<JSONObject> objectOutputTag;
    private final MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //    value:{"db":"","tableName":"","before":{},"after":{},"type":""} type: insert update delete
    // 1、获取广播的配置数据
    // 2、过滤字段，filterColumns
    // 核心处理方法，根据 mysql 配置表的信息为每条数据打上走的标签，走 hbase 还是 kafka
    // 然后 processElement 方法会在主输入流的每个元素上被调用。它用于处理主输入流的每个事件，并使用从广播状态
    // 中获取的数据进行处理，这个方法的执行也是并行的，每个并行子任务都会独立地处理自己结束到的主输入元素
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // broadcastState: {"database":"gmall_flink","before":{},"after":{"name":"da","id":"bbbb"},"type":"insert","tableName":"zzzzz"}
        // 通过调用 getBroadcastState 方法，传入该描述符对象，从 ReadOnlyContext 中获取到广播流的状态对象
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        // 还是没想明白，这里面为什么会有这个key
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            JSONObject data = value.getJSONObject("after");

            // getSinkColumns 输出字段
            filterColumn(data, tableProcess.getSinkColumns());

            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();

            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                // 输出类型为kafka，输出主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                // 为HBase数据，写入侧输出流
                ctx.output(objectOutputTag, value);
            }
        } else {
            // 出现这种情况问题是 tableProcess 表中不存在对应的操作类型
            System.out.println("该组合key: " + key + "不存在！");
        }
    }

    /**
     * @param data        {"id":"11","tm_name":"fd","logo_url":"aaa"}
     * @param sinkColumns sinkColumns 字段来源于 mysql table_Process 表中
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        // 获取 sinkColumns 里面的列的每一个字段
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

        // JSONObject.entrySet() 方法返回一个包含 JSONObject 中所有键值对的 Set 视图
        // 该视图中的每个元素都是一个 Map.Entry 对象，表示一个键值对
        // Set<Map.Entry<String, Object>> entries = data.entrySet();
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }


    /**
     * processBroadcastElement 方法会在广播流的每个元素上被调用，它用于处理广播流中的每个广播事件
     * 并更新状态或发送广播数据给下游处理函数，这个方法的执行时并行的，即每个并行任务都会处理自己接受到的广播元素
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //{
        // "database":"gmall_flink",
        // "before":{},
        // "after":{"operate_type":"update","sink_type":"hbase","sink_table":"dim_zzzzz","source_table":"zzzzz","sink_pk":"id","sink_columns":"id,name"},
        // "type":"insert",
        // "tableName":"table_process"
        // }
        //1.获取并解析数据 返回一个通用的 JSON 对象
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        // 使用了指定目标对象类型的方式进行解析
        // 使用指定目标对象类型的解析可以在解析过程中将 JSON 数据转换为具体的 java 对象
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }
        // 写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuffer createTableSql = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");

            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];

                // 判断是否是主键
                if (sinkPk.equals(field)) {
                    createTableSql.append(field).append(" varchar primary key ");
                } else {
                    createTableSql.append(field).append(" varchar ");
                }

                if (i < fields.length - 1) {
                    createTableSql.append(",");
                }
            }

            // 判断是否是最后一个字段，如果不是则添加 ","
            createTableSql.append(")").append(sinkExtend);
            System.out.println(createTableSql);

            // 预编译 SQL
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
