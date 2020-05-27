import com.ds.kafka.s3sink.partitioner.PartitionFieldExtractor
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionFieldExtractorTest extends AnyFlatSpec with Matchers  {


  "Tenant value " should " have been !!!" in {
    val input = """{"object_data":{"battery_level":100.0,"vehicle_id":346,"alert_type":null,"alert_timestamp":null,"fuel_type":"","event_name":"VEHICLE_STATUS_ALERT","alert_level":null,"vin":"JTDKDTB31J1605923","reservation_status":"free","entity":"vehicle_alert","gps_timestamp":"2019-10-08T14:39:03+00:00","last_known_longitude":-122.30043,"fuel_percentage":82.9,"door_status":"LOCKED","charging_state":"NOT_CHARGING","last_known_latitude":37.89763,"fuel_level":null,"remaining_range":596,"cs_battery_level":null},"metadata":{"sdk_timestamp":"2019-10-08T16:00:00.009489+00:00","version":1,"recorder_received_at":"2019-10-08T16:00:00.298Z","recorder_processed_at":"2019-10-08T16:00:00.326Z"},"tenant_id":"ABC-prod","subtype":null,"type":"SA.OBJECT_CHANGED","data":{},"object_id":346,"object_name":"carsharingvehicle","timestamp":"2019-10-08T15:59:56.417417+00:00","action":"fullsnapshot","name":"VEHICLE_STATUS_ALERT","objects_affected":["Platform"],"description":"Server object changed"}""".stripMargin.getBytes

    val record   = new SinkRecord("topic", 1, null, "", null, input, 1l);
    val epochTime = new PartitionFieldExtractor("tenant_id").extract(record)
    epochTime should be ("ABC-prod")
  }

}
