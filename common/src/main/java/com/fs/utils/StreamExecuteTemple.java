package com.fs.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamExecuteTemple {
    private static StreamExecuteTemple instance;
    public StreamExecuteTemple() {

    }

    public static StreamExecuteTemple getInstance() {
        if (instance == null) {
            synchronized (StreamExecuteTemple.class) {
                if (instance == null) {
                    instance = new StreamExecuteTemple();
                }
            }
        }
        return instance;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecuteTemple streamExecuteTemple = getInstance();
        streamExecuteTemple.app(env);
        env.execute();
    }

    public void app(StreamExecutionEnvironment env) {

    }


}
