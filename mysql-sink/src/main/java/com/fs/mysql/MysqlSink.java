package com.fs.mysql;


import com.fs.bean.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class MysqlSink extends RichSinkFunction<Student> {
    private Properties properties = null;
    private String filePath = null;
    private PreparedStatement ps = null;
    private Connection conn = null;

    private String[] columns = null;
    private String tableName = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        properties = new Properties();
        InputStream in = null;
        try {
            in = this.getClass().getClassLoader().getResourceAsStream("mysql.properties");
            properties.load(in);
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

        String url = properties.getProperty("mysql.url");
        String username = properties.getProperty("mysql.username");
        String driver = properties.getProperty("mysql.driver");
        String password = properties.getProperty("mysql.password");
        String columns = properties.getProperty("mysql.columns");
        String tableName = properties.getProperty("mysql.table");

        int columnsNum= Integer.parseInt(properties.getProperty("mysql.columnsNum"));
        Class.forName(driver);
        StringBuilder sql = new StringBuilder();

        Joiner joiner = Joiner.on(",");
        String[] placeholders = new String[columnsNum];
        Arrays.fill(placeholders, "?");
        String phd = joiner.join(placeholders);

        String insertTableSql = sql.append("INSERT INTO ")
                .append(tableName)
                .append(" (")
                .append(columns)
                .append(") ")
                .append("VALUES ")
                .append("(")
                .append(phd)
                .append(")").toString();


        this.conn = DriverManager.getConnection(url, username, password);
        ps = conn.prepareStatement(insertTableSql);
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        ps.setInt(1,value.getId());
        ps.setInt(2, value.getScore());
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ps != null) ps.close();
        if (conn != null) conn.close();
    }
}
