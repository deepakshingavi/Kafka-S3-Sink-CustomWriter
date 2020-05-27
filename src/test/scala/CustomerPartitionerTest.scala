import java.util.Locale

import com.ds.kafka.s3sink.{TimeStampMapper, CustomerPartitioner}
import org.apache.kafka.connect.sink.SinkRecord
import org.joda.time.DateTimeZone
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConversions._

class CustomerPartitionerTest extends AnyFlatSpec with Matchers  {

  "For 1st input Partition value " should " have been !!!" in {
    val input = """{"object_data":{"battery_level":100.0,"vehicle_id":346,"alert_type":null,"alert_timestamp":null,"fuel_type":"","event_name":"VEHICLE_STATUS_ALERT","alert_level":null,"vin":"JTDKDTB31J1605923","reservation_status":"free","entity":"vehicle_alert","gps_timestamp":"2019-10-08T14:39:03+00:00","last_known_longitude":-122.30043,"fuel_percentage":82.9,"door_status":"LOCKED","charging_state":"NOT_CHARGING","last_known_latitude":37.89763,"fuel_level":null,"remaining_range":596,"cs_battery_level":null},"metadata":{"sdk_timestamp":"2019-10-08T16:00:00.009489+00:00","version":1,"recorder_received_at":"2019-10-08T16:00:00.298Z","recorder_processed_at":"2019-10-08T16:00:00.326Z"},"tenant_id":"ABC-prod","subtype":null,"type":"SA.OBJECT_CHANGED","data":{},"object_id":346,"object_name":"carsharingvehicle","timestamp":"2019-10-08T15:59:56.417417+00:00","action":"fullsnapshot","name":"VEHICLE_STATUS_ALERT","objects_affected":["Platform"],"description":"Server object changed"}""".stripMargin.getBytes
    val record   = new SinkRecord("topic", 1, null, "", null, input, 1l);
    val partitioner = new CustomerPartitioner[String]();
    val configMap = Map[java.lang.String,Object]("directory.delim" -> "/","timestamp.extractor"-> classOf[TimeStampMapper].getName)
    partitioner.init(36000l,"'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH", Locale.US, DateTimeZone.UTC, configMap)
     val partition = partitioner.encodePartition(record)
//    val epochTime = new PartitionFieldExtractor("tenant_id").extract(record)
    partition should be ("tenant_id=ABC-prod/year=2019/month=10/day=08/hour=16")
  }

  "For 2nd input Partition value " should " have been !!!" in {
    val input_2 = """{"tenant_id": "ABC-prod", "timestamp": "2019-11-26T04:10:39.622764+00:00", "object_id": 37914, "object_data": {"id": 37914, "tenant_id": "ABC-prod", "user_id": "ab14caa0-db4d-4099-be5b-0391286c97ba", "consent_given": true, "consent_option_version": 1, "consent_option_name": "Sell me data", "consent_option_description": "I authorize the selling of my Data", "created_at": "2019-11-26T04:10:39.622695922Z", "updated_at": "2019-11-26T04:10:39.622764110Z"}, "object_name": "user-agreements-consent-decisions", "data": {}, "action": "CREATED", "metadata": {"version": 1, "sdk_timestamp": "2019-11-26T04:10:39.641492+00:00"}, "subtype": null, "name": "USER-AGREEMENTS-CONSENT-DECISIONS", "objects_affected": ["Platform"], "description": "Server object changed"}""".stripMargin.getBytes
    val record = new SinkRecord("topic", 1, null, "", null, input_2, 1l);
    val partitioner = new CustomerPartitioner[String]();
    val configMap = Map[java.lang.String, Object]("directory.delim" -> "/", "timestamp.extractor" -> classOf[TimeStampMapper].getName)
    partitioner.init(36000l, "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH", Locale.US, DateTimeZone.UTC, configMap)
    val partition = partitioner.encodePartition(record)
    //    val epochTime = new PartitionFieldExtractor("tenant_id").extract(record)
    partition should be("tenant_id=ABC-prod/year=2019/month=11/day=26/hour=04")
  }

  "For 3nd input Partition value " must " valid timestamp !!!" in {
    val input_2 = """{"tenant_id": "ABC-prod", "type": "SA.OBJECT_CHANGED", "timestamp": "2019-11-26T04:10:39.622764", "object_id": 37914, "object_data": {"id": 37914, "tenant_id": "ABC-prod", "user_id": "ab14caa0-db4d-4099-be5b-0391286c97ba", "consent_given": true, "consent_option_version": 1, "consent_option_name": "Sell me data", "consent_option_description": "I authorize the selling of my Data", "created_at": "2019-11-26T04:10:39.622695922Z", "updated_at": "2019-11-26T04:10:39.622764110Z"}, "object_name": "user-agreements-consent-decisions", "data": {}, "action": "CREATED", "metadata": {"version": 1, "sdk_timestamp": "2019-11-26T04:10:39.641492"}, "subtype": null, "name": "USER-AGREEMENTS-CONSENT-DECISIONS", "objects_affected": ["Platform"], "description": "Server object changed"}""".stripMargin.getBytes
    val record = new SinkRecord("topic", 1, null, "", null, input_2, 1l);
    val partitioner = new CustomerPartitioner[String]();
    val configMap = Map[java.lang.String, Object]("directory.delim" -> "/", "timestamp.extractor" -> classOf[TimeStampMapper].getName)
    partitioner.init(36000l, "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH", Locale.US, DateTimeZone.UTC, configMap)
    val partition = partitioner.encodePartition(record)
    //    val epochTime = new PartitionFieldExtractor("tenant_id").extract(record)
    partition should be("tenant_id=ABC-prod/year=1970/month=01/day=01/hour=00")
  }

}
