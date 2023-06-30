package com.damon.utils;

import com.alibaba.fastjson.JSONObject;
import com.damon.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {

    /**
     * 获取维度信息
     *
     * @param connection connection
     * @param tableName  tableName
     * @param id         primary key
     * @return dimInfoJson
     */
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Jedis jedis = RedisUtil.getJedis();

        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);

        if (dimInfoJsonStr != null) {
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName +
                " where id='" + id + "'";

        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, true);
        JSONObject dimInfoJson = queryList.get(0);

        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception{
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "15"));
        connection.close();
    }

}