package com.fs.db.source;

import com.fs.db.format.JdbcSourceFormat;
import com.fs.utils.JdbcConnectorUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class MysqlSource extends RichParallelSourceFunction<List<Row>> {
    protected JdbcSourceFormat jdbcSourceFormat = null;
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
        jdbcSourceFormat = JdbcConnectorUtil.getMysqlSource(tableName, typeInformation);
        jdbcSourceFormat.openInputFormat();
        jdbcSourceFormat.open(null);
    }

    @Override
    public void run(SourceContext<List<Row>> ctx) throws Exception {
        while (running) {
            ArrayList<Row> rows = new ArrayList<>();
            while (!jdbcSourceFormat.reachedEnd()) {
                Row row = jdbcSourceFormat.nextRecord(new Row(typeInformation.length));
                rows.add(row);
            }
            ctx.collect(rows);
            rows.clear();
            Thread.sleep(delay);
            jdbcSourceFormat.beforeFirst();
        }
    }

    @Override
    public void close() throws Exception {
        jdbcSourceFormat.close();
        jdbcSourceFormat.closeInputFormat();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
