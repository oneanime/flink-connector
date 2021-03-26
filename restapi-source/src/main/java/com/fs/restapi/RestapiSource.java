package com.fs.restapi;


import com.fs.utils.HttpUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

public class RestapiSource extends RichParallelSourceFunction<String> {

    protected String url;
    protected String method;
    protected transient CloseableHttpClient httpClient;
    protected String header;
    protected String requestBody;
    protected long delay;
    private boolean running;

    public RestapiSource(String url, String method, String header, String requestBody, long delay) {
        this.url = url;
        this.method = method;
        this.header = header;
        this.requestBody = requestBody;
        this.delay = delay;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        running = true;
        httpClient = HttpUtil.getHttpClient();
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        HttpUriRequest request = HttpUtil.getRequest(method, requestBody, header, url);
        while (running) {
            try {
                CloseableHttpResponse httpResponse = httpClient.execute(request);
                HttpEntity entity = httpResponse.getEntity();
                if (entity != null) {
                    String entityData = EntityUtils.toString(entity);
                    sourceContext.collect(entityData);
                } else {
                    throw new RuntimeException("entity is null");
                }

            } catch (Exception e) {
                throw new RuntimeException("get entity error");
            }
            Thread.sleep(delay);
        }
    }

    @Override
    public void close() throws Exception {
        HttpUtil.closeClient(httpClient);
        super.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
