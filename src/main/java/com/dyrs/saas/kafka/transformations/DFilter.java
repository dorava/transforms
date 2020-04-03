package com.dyrs.saas.kafka.transformations;

import io.confluent.connect.transforms.Filter;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DFilter<R extends ConnectRecord<R>> extends Filter<R> {
    private static final Logger LOG = LoggerFactory.getLogger(DFilter.class);

    @Override
    protected Schema operatingSchema(R r) {
        return null;
    }

    @Override
    protected Object operatingValue(R r) {
        return null;
    }

    @Override
    public R apply(R record) {
        return super.apply(record);
    }

    @Override
    public void configure(Map<String, ?> props) {
        super.configure(props);
        LOG.info(props.toString());
    }
}
