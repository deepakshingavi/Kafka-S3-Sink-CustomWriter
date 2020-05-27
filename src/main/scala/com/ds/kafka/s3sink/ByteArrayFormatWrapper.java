package com.ds.kafka.s3sink;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.converters.ByteArrayConverter;

import java.util.HashMap;
import java.util.Map;

public class ByteArrayFormatWrapper extends ByteArrayFormat {

    private final S3Storage storage;
    private final ByteArrayConverter converter;

    public ByteArrayFormatWrapper(S3Storage storage) {
        super(storage);
        this.storage = storage;
        this.converter = new ByteArrayConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        this.converter.configure(converterConfig, false);
    }

    public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
        return new ByteArrayRecordWriterProviderExt(storage, converter);
    }

}
