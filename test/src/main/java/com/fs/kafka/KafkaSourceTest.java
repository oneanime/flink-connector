package com.fs.kafka;

import com.fs.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> baseLogDS = env.addSource(KafkaUtil.getKafkaSource("ODS_BASE_LOG", "ods"));
        baseLogDS.print();

        env.execute();
    }
}
