package com.fs.db;

import com.fs.db.source.JdbcSourceUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FormattingMapper;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class MysqlSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> sinkTest = env.createInput(JdbcSourceUtil.getMysqlSource("sink_test",
                new TypeInformation[]{
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                }));
        sinkTest.map(new FormattingMapper<>(new TextOutputFormat.TextFormatter<Row>() {
            @Override
            public String format(Row row) {
                return row.toString();
            }
        })).print();
        env.execute();

    }
}
