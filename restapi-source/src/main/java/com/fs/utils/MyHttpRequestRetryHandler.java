package com.fs.utils;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;


public class MyHttpRequestRetryHandler implements HttpRequestRetryHandler {
    protected static final Logger LOG = LoggerFactory.getLogger(MyHttpRequestRetryHandler.class);

    private int executionMaxCount;

    public MyHttpRequestRetryHandler(Builder builder) {
        this.executionMaxCount = builder.executionMaxCount;
    }

    @Override
    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
        LOG.info("第" + executionCount + "次重试");

        if (executionCount >= this.executionMaxCount) {
            // Do not retry if over max retry count
            return false;
        }
        if (exception instanceof InterruptedIOException) {
            // Timeout
            return true;
        }
        if (exception instanceof UnknownHostException) {
            // Unknown host
            return true;
        }
        if (exception instanceof SSLException) {
            // SSL handshake exception
            return true;
        }
        if (exception instanceof NoHttpResponseException) {
            // No response
            return true;
        }

        HttpClientContext clientContext = HttpClientContext.adapt(context);
        HttpRequest request = clientContext.getRequest();
        boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
        // Retry if the request is considered idempotent
        return !idempotent;
    }


    public static final class Builder {
        private int executionMaxCount;

        public Builder() {
            executionMaxCount = 5;
        }

        public Builder executionCount(int executionCount) {
            this.executionMaxCount = executionCount;
            return this;
        }

        public MyHttpRequestRetryHandler build() {
            return new MyHttpRequestRetryHandler(this);
        }
    }
}
