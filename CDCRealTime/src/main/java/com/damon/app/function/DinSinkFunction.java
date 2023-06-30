package com.damon.app.function;

import com.alibaba.fastjson.JSONObject;
import com.damon.common.GmallConfig;
import com.damon.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DinSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    //value:{"sinkTable":"dim_base_trademark","database":"gmall-210325-flink","before":{"tm_name":"atguigu","id":12},"after":{"tm_name":"Atguigu","id":12},"type":"update","tableName":"base_trademark"}
    //SQL：upsert into db.tn(id,tm_name) values('...','...')

    //{"sinkTable":"dim_user_info",
    // "database":"gmall_flink",
    // "before":{"birthday":"1987-12-04","login_name":"nejo86vvg",
    //           "gender":"F","create_time":"2020-12-04 23:15:59",
    //           "nick_name":"欣欣","name":"费欣","user_level":"1",
    //           "phone_num":"13988373714","id":3542,"email":"nejo86vvg@msn.com"},
    // "after":{"birthday":"1987-12-04 00:00:00","login_name":"nejo86vvg",
    //          "gender":"F","create_time":"2020-12-04 23:15:59",
    //          "name":"费欣","user_level":"1","id":3542,
    //          "operate_time":"2022-08-21 18:23:05"},
    // "type":"update",
    // "tableName":"user_info"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = getUpsertSql(sinkTable, after);
            System.out.println("upsertSql: " + upsertSql);

            preparedStatement = connection.prepareStatement(upsertSql);

            if ("update".equals(value.getString("type"))) {
                DimUtil.delRedisDimInfo(sinkTable, after.getString("id"));
            }
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
                connection.close();
            }
        }
    }

    // data: {"tb_name": "aaa", "id": 12}
    // SQL: upsert into db.tb_name(id, name) values ('', '', '')

    private String getUpsertSql(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
