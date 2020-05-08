package com.dyrs.saas.kafka.transformations;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.transforms.util.Requirements;
import io.confluent.connect.transforms.util.TypeConverter;
import io.confluent.connect.utils.Strings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class CastValue<R extends ConnectRecord<R>> implements Transformation<R> {


    @Override
    public R apply(R record) {
        if (record.valueSchema() == null || !record.valueSchema().name().endsWith(".Envelope")) {
            return record;
        }
        Object data = TypeConverter.convertObject(Requirements.requireStruct(record.value(), "filtering record with schema"), ((Struct) record.value()).schema());

        Struct struct = (Struct) record.value();
        EncodeValue(record, struct, "after");
        EncodeValue(record, struct, "before");

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), struct, record.timestamp());
    }

    private void EncodeValue(R record, Struct struct, String key) {
        Schema schema = record.valueSchema().field(key).schema();
        Struct value = struct.getStruct(key);
        if (value != null) {
            for (Field field : schema.fields()) {
                try {
                    if (field.schema().type() == Schema.Type.STRING) {
                        String name = field.name();
                        String old = value.getString(name);
                        System.out.println(name + ": " + old);
                        if (!Strings.isNullOrEmpty(old)) {
                            value.put(field, URLEncoder.encode(old, "UTF-8"));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
//            struct.put("after", after);
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
