package com.ds.kafka.s3sink

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.{ChronoField, TemporalAccessor}
import java.util

import com.fasterxml.jackson.core.JsonParseException
import io.confluent.connect.storage.partitioner.TimestampExtractor
import org.apache.kafka.connect.connector.ConnectRecord
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{parse, render}
import org.slf4j.LoggerFactory

class TimeStampMapper extends TimestampExtractor {

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val formats = DefaultFormats
  val dateTimeFormatInputData: DateTimeFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive
    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .appendOptional(new DateTimeFormatterBuilder().appendPattern(".SSSSSS").toFormatter())
    .appendOptional(new DateTimeFormatterBuilder().appendOffsetId.toFormatter)
//    .appendOffsetId
    .toFormatter()

  override def extract(record: ConnectRecord[_]): java.lang.Long = {
    try {
      var eventTimeStampStr = render(parse(record.value().asInstanceOf[Array[Byte]].map(_.toChar).mkString)
        \ "metadata" \ "sdk_timestamp").extractOrElse[String]("")

      eventTimeStampStr = if ("".equals(eventTimeStampStr)) {
        render(parse(record.value().asInstanceOf[Array[Byte]].map(_.toChar).mkString) \ "timestamp").extractOrElse[String]("")
      } else {
        eventTimeStampStr
      }

      eventTimeStampStr = if ("".equals(eventTimeStampStr)) {
        render(parse(record.value().asInstanceOf[Array[Byte]].map(_.toChar).mkString)
          \ "data" \ "timestamp").extractOrElse[String]("")
      } else {
        eventTimeStampStr
      }

      val eventDateTime : TemporalAccessor = dateTimeFormatInputData.parse(eventTimeStampStr) //1570550400009
      (eventDateTime.getLong(ChronoField.INSTANT_SECONDS) * 1000L) + eventDateTime.getLong(ChronoField.MILLI_OF_SECOND)
    } catch {
      case e: JsonParseException =>
        val recordValue = record
          .value()
          .asInstanceOf[Array[Byte]]
          .map(_.toChar)
          .mkString

        logger.error(s"Invalid JSON detected, sending to epoch=$recordValue", e)
        0L
      case me @ (_ :org.json4s.MappingException | _ : java.time.format.DateTimeParseException
        | _ : java.time.temporal.UnsupportedTemporalTypeException) =>
        val recordValue = record
          .value()
          .asInstanceOf[Array[Byte]]
          .map(_.toChar)
          .mkString
        logger.error(s"Error parsing timestamp from recordValue=$recordValue !!!", me)
        0L
      case e: Exception =>
        val recordValue = record
          .value()
          .asInstanceOf[Array[Byte]]
          .map(_.toChar)
          .mkString
        logger.error(s"Error parsing the record=${record} and recordValue=$recordValue !!!", e)
        throw e
    }
  }

  def configure(map: util.Map[String, Object]): Unit = {}
}
