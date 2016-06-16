package fr.yuzutech.kafka.connect.elasticsearch;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ElasticsearchSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);

    private ElasticsearchHTTPClient client;
    private RecordReader reader;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
            this.client = new ElasticsearchHTTPClient(config);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start " + ElasticsearchSinkConnector.class.getName() + " due to configuration error.", e);
        }
        this.reader = new RecordReader();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            try {
                String value = reader.readValue(record);
                log.trace("Processing {}", value);
                // TODO Flush after X seconds if no new record is sent
                client.bulk(value);
            } catch (IOException e) {
                throw new RetriableException("Error while indexing data");
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    }

    @Override
    public void stop() {
        try {
            client.flush();
        } catch (IOException e) {
            throw new ConnectException("Error while indexing remaining data");
        }
    }
}
