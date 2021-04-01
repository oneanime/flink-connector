package com.fs.kafka;

import com.fs.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaTableTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        String options = ",'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'";


        String create_sql = "CREATE TABLE user_behavior (\n" +
                "  `userId` BIGINT,\n" +
                "  `itemId` BIGINT,\n" +
                "  `category` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `timestamp` STRING \n" +
                ") " + KafkaUtil.getKafkaJsonDDL("user_action", "test", "earliest-offset",options);

        String pv_sql="select userId,count(1) from user_behavior where `behavior`='pv' group by `userId`";
        String query_sql = "select *from user_behavior";
        TableResult tableResult = tEnv.executeSql(create_sql);
        Table sqlQuery = tEnv.sqlQuery(pv_sql);
        tEnv.toRetractStream(sqlQuery, Row.class).print();
        env.execute();
    }
}
