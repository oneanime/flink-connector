package com.fs.db;

import com.fs.db.sink.JdbcSinkUtil;
import com.fs.utils.TestSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
public class MysqlSinkTest {
    public static void main(String[] args) throws Exception {

        SingleOutputStreamOperator restSource = TestSource.restSource();
        restSource.print();
        restSource.addSink(JdbcSinkUtil.getMysqlSink("insert into user_action_info(user_id, item_id, category, behavior, ts) values (?,?,?,?,?)"));

        restSource.getExecutionEnvironment().execute();

    }
}
