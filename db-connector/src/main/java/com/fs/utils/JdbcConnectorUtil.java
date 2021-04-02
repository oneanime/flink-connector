package com.fs.utils;

import com.fs.db.format.JdbcSourceFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class JdbcConnectorUtil {
    private static Properties prop = null;
    private static final String CONFIG_FILE_PATH = "config.properties";

    static {
        InputStream in = null;
        try {
            prop = new Properties();
            in = JdbcConnectorUtil.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH);
            prop.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert in != null;
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

/*********************sink*************************************************/

    public static <T> SinkFunction getMysqlSink(String sql) {
        JdbcStatementBuilder<T> jdbcStatementBuilder = new JdbcStatementBuilder<T>() {

            ObjectMapper objectMapper = new ObjectMapper();
            String json = null;
            JavaType mapType = objectMapper.getTypeFactory().constructParametricType(LinkedHashMap.class, String.class, String.class);
            Map<String, String> map = null;

            @Override
            public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                try {
                    json = objectMapper.writeValueAsString(obj);
                    map = objectMapper.readValue(json, mapType);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                int index = 1;
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    preparedStatement.setObject(index, entry.getValue());
                    index++;
                }
                map.clear();
            }
        };

        JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder().withBatchSize(5).build();
        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(prop.getProperty("mysql.driver"))
                .withUrl(prop.getProperty("mysql.url"))
                .withUsername(prop.getProperty("mysql.username"))
                .withPassword(prop.getProperty("mysql.password"))
                .build();
        return JdbcSink.sink(sql, jdbcStatementBuilder, jdbcExecutionOptions, jdbcConnectionOptions);
    }

/***********************source*********************************************/

    public static JdbcInputFormat getJdbcSource(String url, String driver, String username, String password, String sql, TypeInformation[] fieldTypes, Boolean isAutoCommit) {

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDBUrl(url)
                .setDrivername(driver)
                .setUsername(username)
                .setPassword(password)
                .setQuery(sql)
                .setRowTypeInfo(rowTypeInfo)
                .setAutoCommit(isAutoCommit)
                .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
                .finish();
        return jdbcInputFormat;
    }

    public static JdbcInputFormat getMysqlSource(String tableName, TypeInformation[] fieldTypes){
        String mysqlUrl=prop.getProperty("mysql.url");
        String mysqlDriver = prop.getProperty("mysql.driver");
        String mysqlUserName = prop.getProperty("mysql.username");
        String mysqlPwd = prop.getProperty("mysql.password");
        String sql = String.format("select * from %s",tableName);
        JdbcInputFormat mysqlSource = getJdbcSource(mysqlUrl, mysqlDriver, mysqlUserName, mysqlPwd, sql, fieldTypes, true);
        return mysqlSource;
    }


/********************DDL**************************************************/

    public static String getJdbcDDL(String url,String tableName,String driver,String username,String password) {
        StringBuilder builder = new StringBuilder();
        StringBuilder ddl = builder.append("WITH (")
                .append("'connector' = 'jdbc',")
                .append("'url' = '%s',")
                .append("'table-name' = '%s',")
                .append("'driver' = '%s',")
                .append("'username' = '%s',")
                .append("'password' = '%s'")
                .append(" %s")
                .append(")");
        return String.format(ddl.toString(),url,tableName,driver,username,password);
    }

    public static String getMysqlDDL(String tableName) {
        String mysqlUrl=prop.getProperty("mysql.url");
        String mysqlDriver = prop.getProperty("mysql.driver");
        String mysqlUserName = prop.getProperty("mysql.username");
        String mysqlPwd = prop.getProperty("mysql.password");
        return getJdbcDDL(mysqlUrl, tableName, mysqlDriver, mysqlUserName, mysqlPwd);
    }
}
