package com.fs.db.source;

import com.fs.utils.JdbcConnectorUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class MysqlSource extends RichParallelSourceFunction<List<Row>> {
    protected JdbcInputFormat jdbcInputFormat = null;
    protected Boolean running = true;
    protected long delay;
    protected TypeInformation[] typeInformation;
    private String tableName;

    public MysqlSource(String tableName, TypeInformation[] typeInformation, Long delay) {
        this.tableName = tableName;
        this.typeInformation = typeInformation;
        this.delay = delay;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jdbcInputFormat = JdbcConnectorUtil.getMysqlSource(tableName, typeInformation);
        jdbcInputFormat.openInputFormat();
    }

    @Override
    public void run(SourceContext<List<Row>> ctx) throws Exception {
        while (running) {

            // open()方法中会调用resultSet = statement.executeQuery();去查询数据库
            jdbcInputFormat.open(null);
            ArrayList<Row> rows = new ArrayList<>();
            while (!jdbcInputFormat.reachedEnd()) {
                Row row = jdbcInputFormat.nextRecord(new Row(typeInformation.length));
                rows.add(row);
            }
            ctx.collect(rows);
            rows.clear();
            Thread.sleep(delay);
        }
    }

    @Override
    public void close() throws Exception {
        jdbcInputFormat.close();
        jdbcInputFormat.closeInputFormat();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
