package com.ds.kafka.s3sink;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ByteArrayRecordWriterProviderExt implements RecordWriterProvider<S3SinkConnectorConfig> {


    private static final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProviderExt.class);
    private final S3Storage storage;
    private final ByteArrayConverter converter;
    private final String extension;
    private final byte[] lineSeparatorBytes;
    private final Map<String,Object> kafkaProps;
    private final String kafkaTopic;

    private static final String S3_INFO_KAFKA_TOPIC = "pushed-files";
    private static final String S3_INFO_KAFKA_HOST = "localhost:9092";

    public ByteArrayRecordWriterProviderExt(S3Storage storage, ByteArrayConverter converter) {
        this.storage = storage;
        this.converter = converter;
        this.extension = storage.conf().getByteArrayExtension();
        this.lineSeparatorBytes = storage.conf()
                .getFormatByteArrayLineSeparator()
                .getBytes(StandardCharsets.UTF_8);
        this.kafkaProps = new HashMap<>();
        this.kafkaTopic = System.getenv().get(S3_INFO_KAFKA_TOPIC);
        this.kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,System.getenv().get(S3_INFO_KAFKA_HOST));
        this.kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        BasicConfigurator.configure();
    }

    @Override
    public String getExtension() {
        return extension + storage.conf().getCompressionType().extension;
    }

    @Override
    public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
        return new RecordWriter() {

            final KafkaProducer<String,String> s3FileNamePublisher = new KafkaProducer<>(kafkaProps);
            final S3OutputStream s3out = storage.create(filename, true);
            final OutputStream s3outWrapper = s3out.wrapForCompression();

            @Override
            public void write(SinkRecord record) {
                log.trace("Custom writer Sink record: {}", record);
                try {
                    byte[] bytes = converter.fromConnectData(
                            record.topic(), record.valueSchema(), record.value());
                    s3outWrapper.write(bytes);
                    s3outWrapper.write(lineSeparatorBytes);
                } catch (IOException | DataException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void commit() {
                try {
                    log.trace("Custom writer commit before {}", filename);
                    s3out.commit();
                    s3outWrapper.close();
                    String s3FullFileName = String.format("s3://%s/%s", storage.conf().getBucketName(), filename);
                    ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, s3FullFileName, s3FullFileName);
                    s3FileNamePublisher.send(record);
                    log.info("File saved to S3 fileName={}",s3FullFileName);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
                catch (Exception e) {
                    log.error("Custom writer : ", e);
                    throw e;
                }
            }

            @Override
            public void close() {
            }
        };

    }
}
