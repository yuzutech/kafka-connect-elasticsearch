package org.yuzutech.kafka.connect.elasticsearch;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchHTTPClient {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchHTTPClient.class);

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final MediaType BINARY = MediaType.parse("application/x-www-form-urlencoded; charset=utf-8");

    private OkHttpClient client;

    private final String uri;
    private final int bulkSize;
    private final String bulkIndexLine;

    private List<String> bulkData = new ArrayList<>();

    public ElasticSearchHTTPClient(ElasticSearchSinkConnectorConfig config) {
        this.client = new OkHttpClient();
        String protocol = config.getProtocol();
        String host = config.getHost();
        Integer port = config.getPort();
        this.uri = protocol + "://" + host + ":" + port;
        String indexName = config.getIndexName();
        String indexType = config.getIndexType();
        this.bulkSize = config.getBulkSize();
        this.bulkIndexLine = "{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"" + indexType + "\" } }\n";
    }

    public void bulk(String json) throws IOException {
        bulkData.add(json);
        if (bulkData.size() == bulkSize) {
            flush();
        }
    }

    public String buildBulkData() {
        StringBuilder sb = new StringBuilder(bulkSize * bulkIndexLine.length() + bulkSize * 100);
        for (String data: bulkData) {
            sb.append(bulkIndexLine);
            sb.append(data);
            sb.append("\n");
        }
        return sb.toString();
    }

    public void flush() throws IOException {
        String data = buildBulkData();
        log.trace("Sending bulk " + data);
        RequestBody body = RequestBody.create(BINARY, data);
        Request request = new Request.Builder()
                .url(uri + "/_bulk")
                .post(body)
                .build();
        client.newCall(request).execute();
        bulkData.clear();
    }
}
