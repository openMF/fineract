package org.apache.fineract.client.feign;

import feign.Client;
import feign.Feign;
import feign.Request;
import feign.Retryer;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.codec.ErrorDecoder;
import feign.httpclient.ApacheHttp5Client;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.slf4j.Slf4jLogger;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.util.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * Configuration class for Feign client.
 */
public class FineractFeignClientConfig {

    private final String baseUrl;
    private final String username;
    private final String password;
    private final int connectTimeout;
    private final int readTimeout;
    private final boolean debugEnabled;

    private FineractFeignClientConfig(Builder builder) {
        this.baseUrl = builder.baseUrl;
        this.username = builder.username;
        this.password = builder.password;
        this.connectTimeout = builder.connectTimeout;
        this.readTimeout = builder.readTimeout;
        this.debugEnabled = builder.debugEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public <T> T createClient(Class<T> apiType) {
        return Feign.builder()
                .client(createHttpClient())
                .encoder(createEncoder())
                .decoder(createDecoder())
                .errorDecoder(createErrorDecoder())
                .options(new Request.Options(connectTimeout, TimeUnit.MILLISECONDS, readTimeout, TimeUnit.MILLISECONDS, true))
                .retryer(Retryer.NEVER_RETRY)
                .requestInterceptor(new BasicAuthRequestInterceptor(username, password))
                .logger(new Slf4jLogger(apiType))
                .logLevel(debugEnabled ? feign.Logger.Level.FULL : feign.Logger.Level.BASIC)
                .target(apiType, baseUrl);
    }

    private Client createHttpClient() {
        PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
                .setMaxConnTotal(200)
                .setMaxConnPerRoute(20)
                .build();

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(connectTimeout))
                        .setResponseTimeout(Timeout.ofMilliseconds(readTimeout))
                        .build())
                .build();

        return new ApacheHttp5Client(httpClient);
    }

    private Encoder createEncoder() {
        return new JacksonEncoder(ObjectMapperFactory.create());
    }

    private Decoder createDecoder() {
        return new JacksonDecoder(ObjectMapperFactory.create());
    }

    private ErrorDecoder createErrorDecoder() {
        return new FineractErrorDecoder();
    }

    public static class Builder {
        private String baseUrl;
        private String username;
        private String password;
        private int connectTimeout = 30000; // 30 seconds
        private int readTimeout = 60000;    // 60 seconds
        private boolean debugEnabled = false;

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder connectTimeout(int timeout, TimeUnit unit) {
            this.connectTimeout = (int) unit.toMillis(timeout);
            return this;
        }

        public Builder readTimeout(int timeout, TimeUnit unit) {
            this.readTimeout = (int) unit.toMillis(timeout);
            return this;
        }

        public Builder debugEnabled(boolean debugEnabled) {
            this.debugEnabled = debugEnabled;
            return this;
        }

        public FineractFeignClientConfig build() {
            return new FineractFeignClientConfig(this);
        }
    }
}
