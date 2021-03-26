package com.fs.kafka;

import com.fs.utils.KafkaUtil;
import com.fs.utils.TestSource;
import com.google.gson.Gson;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator source = TestSource.restSource();

        source.map(data -> {
            Gson gson = new Gson();
            return gson.toJson(data).toString();
        }).addSink(KafkaUtil.getKafkaSink("user_action"));

        source.print();
        source.getExecutionEnvironment().execute();
    }
}
