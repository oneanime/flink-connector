package com.fs.db;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class JdbcSinkUtils {
    private static Properties prop = null;
    private static final String CONFIG_FILE_PATH = "jdbc.properties";

    static {
        InputStream in = null;
        try {
            prop = new Properties();
            in = JdbcSinkUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH);
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

    public static <T> SinkFunction getPhoenixSink(String sql){
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
                .withDriverName(prop.getProperty("phoenix.driver"))
                .withUrl(prop.getProperty("phoenix.url"))
                .build();
        return JdbcSink.sink(sql, jdbcStatementBuilder, jdbcExecutionOptions, jdbcConnectionOptions);
    }

}



