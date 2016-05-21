package org.yuzutech.kafka.connect.elasticsearch;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.IOException;

public class ElasticSearchHTTPClient {

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private OkHttpClient client;

    private final String uri;
    private final String indexName;
    private final String indexType;

    public ElasticSearchHTTPClient(ElasticSearchSinkConnectorConfig config) {
        this.client = new OkHttpClient();
        String protocol = config.getProtocol();
        String host = config.getHost();
        Integer port = config.getPort();
        this.uri = protocol + "://" + host + ":" + port;
        this.indexName = config.getIndexName();
        this.indexType = config.getIndexType();
    }

    public void index(String json) throws IOException {
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url(uri + "/" + indexName + "/" + indexType)
                .post(body)
                .build();
        client.newCall(request).execute();
    }
}
