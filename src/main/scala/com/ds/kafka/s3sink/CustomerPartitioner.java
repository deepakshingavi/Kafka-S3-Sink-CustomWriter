package com.ds.kafka.s3sink;

import com.ds.kafka.s3sink.partitioner.PartitionFieldExtractor;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class CustomerPartitioner<T> extends HourlyPartitioner<T> {

    private static final Logger log = LoggerFactory.getLogger(CustomerPartitioner.class);

    private long partitionDurationMs;
    private DateTimeFormatter formatter;
    private TimestampExtractor timestampExtractor;

    private final String parttitionFieldName = "tenant_id";

    private PartitionFieldExtractor partitionFieldExtractor;

    public void init(long partitionDurationMs, String pathFormat, Locale locale, DateTimeZone timeZone, Map<String, Object> config) {

        this.delim = (String)config.get("directory.delim");


        this.partitionDurationMs = TimeUnit.HOURS.toMillis(1L);
        log.info("old partitionDurationMs = "+partitionDurationMs+" new partitionDurationMs= "+this.partitionDurationMs);

        try {
            this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
            this.timestampExtractor = this.newTimestampExtractor((String)config.get("timestamp.extractor"));
            this.timestampExtractor.configure(config);
            this.partitionFieldExtractor = new PartitionFieldExtractor(parttitionFieldName);
        } catch (IllegalArgumentException e) {

            ConfigException ce = new ConfigException("path.format", pathFormat, e.getMessage());
            ce.initCause(e);
            throw ce;

        }
    }

    private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
        return DateTimeFormat.forPattern(str).withZone(timeZone);
    }

    public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {

        long adjustedTimestamp = timeZone.convertUTCToLocal(timestamp);
        long partitionedTime = adjustedTimestamp / timeGranularityMs * timeGranularityMs;

        return timeZone.convertLocalToUTC(partitionedTime, false);

    }

    public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
        try {
            final Long timestamp = this.timestampExtractor.extract(sinkRecord, nowInMillis);
            final String partitionField = this.partitionFieldExtractor.extract(sinkRecord);
            return this.encodedPartitionForFieldAndTime(sinkRecord, timestamp, partitionField);
        } catch (Exception ex) {
            log.error("Error parsing record value="+new String((byte[])sinkRecord.value()),ex);
            throw ex;
        }
    }

    public String encodePartition(SinkRecord sinkRecord) {
        try {
            final Long timestamp = this.timestampExtractor.extract(sinkRecord);
            final String partitionFieldValue = this.partitionFieldExtractor.extract(sinkRecord);
            return encodedPartitionForFieldAndTime(sinkRecord, timestamp, partitionFieldValue);
        } catch (Exception ex) {
            log.error("Error parsing record value="+new String((byte[])sinkRecord.value()),ex);
            throw ex;
        }
    }

    private String encodedPartitionForFieldAndTime(SinkRecord sinkRecord, Long timestamp, String partitionField) {

        if (timestamp == null) {

            String msg = "Unable to determine timestamp using timestamp.extractor " + this.timestampExtractor.getClass().getName() + " for record: " + sinkRecord;
            log.error(msg);
            throw new ConnectException(msg);

        } else if (partitionField == null) {

            String msg = "Unable to determine partition field using partition.field '" + partitionField  + "' for record: " + sinkRecord;
            log.error(msg);
            throw new ConnectException(msg);

        }  else {
            DateTime bucket = new DateTime(getPartition(this.partitionDurationMs, timestamp.longValue(), this.formatter.getZone()));
            String partion = parttitionFieldName + "=" + partitionField + this.delim + bucket.toString(this.formatter);
            log.info("partitionField = "+partitionField+" this.delim = "+this.delim+" bucket.toString(this.formatter) = " + bucket.toString(this.formatter));
            log.info("partion = " + partion);
            return partion;

        }
    }
}
