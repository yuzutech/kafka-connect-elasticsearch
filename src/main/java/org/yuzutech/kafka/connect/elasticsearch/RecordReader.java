package org.yuzutech.kafka.connect.elasticsearch;

import org.apache.kafka.connect.connector.ConnectRecord;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class RecordReader {

    private final org.apache.kafka.connect.json.JsonConverter valueConverter;

    public RecordReader() {
        this.valueConverter = new org.apache.kafka.connect.json.JsonConverter();
        Map<String, String> props = new HashMap<>();
        props.put("schemas.enable", Boolean.FALSE.toString());
        valueConverter.configure(props, false);
    }

    public String readValue(ConnectRecord connectRecord) {
        byte[] data = this.valueConverter.fromConnectData(connectRecord.topic(), connectRecord.valueSchema(), connectRecord.value());
        return new String(data, StandardCharsets.UTF_8);
    }
}
