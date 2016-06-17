package fr.yuzutech.kafka.connect.elasticsearch;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import rx.Subscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ElasticsearchHTTPClient {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchHTTPClient.class);

    private static final MediaType BINARY = MediaType.parse("application/x-www-form-urlencoded; charset=utf-8");

    private OkHttpClient client;

    private final String uri;
    private final int bulkSize;
    private final String bulkIndexLine;
    private final ReactiveBuffer reactiveBuffer;

    public ElasticsearchHTTPClient(ElasticsearchSinkConnectorConfig config) {
        this.client = new OkHttpClient();
        String protocol = config.getProtocol();
        String host = config.getHost();
        Integer port = config.getPort();
        this.uri = protocol + "://" + host + ":" + port;
        String indexName = config.getIndexName();
        String indexType = config.getIndexType();
        this.bulkSize = config.getBulkSize();
        this.bulkIndexLine = "{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"" + indexType + "\" } }\n";
        this.reactiveBuffer = new ReactiveBuffer(config.getIdleFlushTime(), TimeUnit.MILLISECONDS, bulkSize);
        this.reactiveBuffer.getBuffer().subscribe(new Subscriber<List<String>>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onNext(List<String> values) {
                if (values == null || values.isEmpty()) {
                    return;
                }
                try {
                    String data = buildBulkData(values);
                    log.trace("Sending bulk " + data);
                    RequestBody body = RequestBody.create(BINARY, data);
                    Request request = new Request.Builder()
                        .url(uri + "/_bulk")
                        .post(body)
                        .build();
                    log.info("Sending " + values.size() + " records");
                    client.newCall(request).execute();
                } catch (IOException e) {
                    log.error("Error while indexing data", e);
                }
            }
        });
    }

    public void bulk(String value) {
        this.reactiveBuffer.put(value);
    }

    public void stop() {
        this.reactiveBuffer.onCompleted();
    }

    private String buildBulkData(List<String> bulkData) {
        StringBuilder sb = new StringBuilder(bulkSize * bulkIndexLine.length() + bulkSize * 100);
        for (String data : bulkData) {
            sb.append(bulkIndexLine);
            sb.append(data);
            sb.append("\n");
        }
        return sb.toString();
    }
}
