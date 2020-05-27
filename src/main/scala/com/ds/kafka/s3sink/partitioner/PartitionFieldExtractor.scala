package com.ds.kafka.s3sink.partitioner

import com.fasterxml.jackson.core.JsonParseException
import io.confluent.connect.storage.errors.PartitionException
import org.apache.kafka.connect.connector.ConnectRecord
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{parse, render}
import org.slf4j.LoggerFactory

class PartitionFieldExtractor(val fieldName: String) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val fixedErrorPartition = "ERROR_PARTITION"

  implicit val formats = DefaultFormats

  def extract(record: ConnectRecord[_]): String = {
    try {
      val value = record.value().asInstanceOf[Array[Byte]].map(_.toChar).mkString
      val partitionValue = Some(render(parse(value) \ fieldName).extract[String]).getOrElse(throw new PartitionException(s"Error ${fieldName} value not set for ${value}."))
      partitionValue.asInstanceOf[String]
    } catch {
      case ex: JsonParseException =>
        logger.error(s"Invalid JSON detected, using error-partition. json value=${record.value().asInstanceOf[Array[Byte]].map(_.toChar).mkString}", ex)
        fixedErrorPartition
    }
  }
}
