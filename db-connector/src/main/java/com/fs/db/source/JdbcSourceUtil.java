package com.fs.db.source;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class JdbcSourceUtil {

    private static Properties prop = null;
    private static final String CONFIG_FILE_PATH = "jdbc.properties";

    static {
        InputStream in = null;
        try {
            prop = new Properties();
            in = JdbcSourceUtil.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH);
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
