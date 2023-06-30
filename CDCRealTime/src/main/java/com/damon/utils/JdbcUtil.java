package com.damon.utils;

import com.alibaba.fastjson.JSONObject;
import com.damon.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class JdbcUtil {

    /**
     * @param connection        connection
     * @param querySql          query sql
     * @param clz               对应传入的类
     * @param underScoreToCamel true/false
     * @param <T>               泛型
     * @return ArrayList columnName value
     */
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        ArrayList<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        ResultSet resultSet = preparedStatement.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            // 创建泛型对
            T t = clz.newInstance();

            for (int i = 0; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE
                            .to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                Object value = resultSet.getObject(i);
                // 给泛型对赋值
                BeanUtils.setProperty(t, columnName, value);
            }

            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();
        return resultList;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> queryList = queryList(
                connection,
                "select * from GMALL210325_REALTIME.DIM_USER_INFO",
                JSONObject.class,
                true
        );

        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }
        connection.close();
    }
}
