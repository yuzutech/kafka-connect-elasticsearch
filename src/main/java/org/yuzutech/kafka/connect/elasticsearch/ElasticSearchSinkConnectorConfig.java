package org.yuzutech.kafka.connect.elasticsearch;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.Type;

public class ElasticSearchSinkConnectorConfig extends AbstractConfig {

    public static final String PROTOCOL = "protocol";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String INDEX = "index";
    private static final String INDEX_DOC = "The name of elasticsearch index. Default is: kafka";

    public static final String TYPE = "type";
    private static final String TYPE_DOC = "The type of elasticsearch index.";

    public static final String BULK_SIZE = "bulk.size";
    private static final String BULK_SIZE_DOC = "The number of messages to be bulk indexed into elasticsearch. Default is: 1000";

    static ConfigDef config = new ConfigDef()
            .define(PROTOCOL, Type.STRING, "http", Importance.HIGH, "")
            .define(HOST, Type.STRING, "localhost", Importance.HIGH, "")
            .define(PORT, Type.INT, 9200, Importance.HIGH, "")
            .define(USERNAME, Type.STRING, "", Importance.HIGH, "")
            .define(PASSWORD, Type.STRING, "", Importance.HIGH, "")
            .define(INDEX, Type.STRING, "kafka-index", Importance.HIGH, INDEX_DOC)
            .define(TYPE, Type.STRING, "", Importance.HIGH, TYPE_DOC)
            .define(BULK_SIZE, Type.INT, 1000, Importance.HIGH, BULK_SIZE_DOC);

    public ElasticSearchSinkConnectorConfig(Map<String, String> props) {
        super(config, props);
    }

    public String getProtocol() {
        return getString(PROTOCOL);
    }

    public String getHost() {
        return getString(HOST);
    }

    public Integer getPort() {
        return getInt(PORT);
    }

    public String getIndexName() {
        String indexName = getString(INDEX);
        MessageFormat messageFormat = new MessageFormat(indexName);
        Object[] arguments = {new Date()};
        return messageFormat.format(arguments);
    }

    public String getIndexType() {
        return getString(TYPE);
    }

    public Integer getBulkSize() {
        return getInt(BULK_SIZE);
    }
}
