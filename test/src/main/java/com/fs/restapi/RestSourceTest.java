package com.fs.restapi;

import com.fs.utils.TestSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class RestSourceTest extends TestSource{
    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator userActionDS = TestSource.restSource();
        userActionDS.print();
        userActionDS.getExecutionEnvironment().execute();

    }

}
