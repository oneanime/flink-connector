package com.fs.db;


import com.fs.utils.TestSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class PhoenixTest {
    public static void main(String[] args) throws Exception {
//        SingleOutputStreamOperator restSource = TestSource.restSource();
//        restSource.addSink(JdbcSinkUtils.getPhoenixSink("insert into user_action_info(user_id, item_id, category, behavior, ts) values (?,?,?,?,?)"));
        DataStreamSource source = TestSource.studSource();

        source.addSink(JdbcSinkUtil.getPhoenixSink("UPSERT INTO STUDENT (ID,SCORE) VALUES(?,?)"));
        source.getExecutionEnvironment().execute();
    }
}
