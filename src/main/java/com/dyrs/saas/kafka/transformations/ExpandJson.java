package com.dyrs.saas.kafka.transformations;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.transforms.util.Requirements;
import io.confluent.connect.transforms.util.TypeConverter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.HashMap;
import java.util.Map;

public class ExpandJson<R extends ConnectRecord<R>> implements Transformation<R> {
    private JsonConverter converter;

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, String> config = new HashMap<>();
        config.put("converter.type", "value");
        config.put("schemas.enable", "false");

        converter = new JsonConverter();
        converter.configure(config);
    }

    @Override
    public R apply(R record) {
        if (record.valueSchema() == null || !record.valueSchema().name().endsWith(".Envelope")) {
            return record;
        }
        Object data = TypeConverter.convertObject(Requirements.requireStruct(record.value(), "filtering record with schema"), ((Struct) record.value()).schema());

        Struct struct = (Struct) record.value();
        String op = struct.getString("op");

        if (op.equals("r") || op.equals("c") || op.equals("u")) {
            Struct after = struct.getStruct("after");
//            Object id = after.get("id");
//            String rootType = after.getString("rootType");
//            String keySchema = after.getString("keySchema");
//            String valueSchema = after.getString("valueSchema");
//            String materialization = after.getString("materialization");
            String json = "{}";
            try {
                json = new ObjectMapper().writeValueAsString(data);

            } catch (Exception ex) {

            }
            String valueEnvelope = "{\"payload\" : " + json + "}";

            SchemaAndValue value = converter.toConnectData("dummy", valueEnvelope.getBytes());

//            String keyEnvelope = "{ " + keySchema.substring(2, keySchema.length() - 2) + ", \"payload\" : " + id
//                    + "}";

//            SchemaAndValue key = converter.toConnectData("dummy", keyEnvelope.getBytes());

            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(),
                    value.value(), record.timestamp());
        } else if (op.equals("d")) {
            Struct before = struct.getStruct("before");
            Object id = before.get("rootId");
            String rootType = before.getString("rootType");
            String keySchema = before.getString("keySchema");

            String keyEnvelope = "{ " + keySchema.substring(2, keySchema.length() - 2) + ", \"payload\" : " + id
                    + "}";

            SchemaAndValue key = converter.toConnectData("dummy", keyEnvelope.getBytes());

            return record.newRecord(rootType, record.kafkaPartition(), key.schema(), key.value(), null, null,
                    record.timestamp());
        } else {
            throw new IllegalArgumentException("Unexpected record type: " + record);
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        converter.close();
    }
}
