package fr.yuzutech.kafka.connect.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ElasticsearchSinkConnectorConfigTest {

    @Test
    public void should_format_indexName_with_date() {
        Map<String, String> props = new HashMap<>();
        props.put(ElasticsearchSinkConnectorConfig.INDEX, "connect-{0,date,YYYY-MM-dd}");
        ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
        String indexName = config.getIndexName();
        assertThat(indexName).isEqualTo("connect-" + new SimpleDateFormat("YYYY-MM-dd").format(new Date()));
    }

    @Test
    public void should_format_indexName_without_date() {
        Map<String, String> props = new HashMap<>();
        props.put(ElasticsearchSinkConnectorConfig.INDEX, "connect");
        ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
        String indexName = config.getIndexName();
        assertThat(indexName).isEqualTo("connect");
    }
}
