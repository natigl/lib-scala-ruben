package com.gestamp.hypatia.utils

object ConstantsKafka {

  val SOURCE_PARAM = "source"
  val DATASET_PARAM = "dataset"

  val KAFKA = "kafka"
  val KAFKA_SERVER = "kafka.bootstrap.servers"
  val SUBSCRIBE = "subscribe"
  val STARTING_OFFSET = "startingOffsets"
  val FAIL_ON_DATA_LOSS = "failOnDataLoss"
  val DATA = "data"
  val TIMESTAMP = "timestamp"
  val TRANSLATED_TIMESTAMP = "kafka_timestamp"
  val JSON_DATA = "json_data"
  val VALUE = "value"
  val TOPIC = "topic"
  val METADATA = "_metadata"

  val FULL_EXTRACTION = "FULL"
  val DELTA_EXTRACTION = "DELTA"
  val CDC_EXTRACTION = "CDC"
  val FULL_PARTITION_EXTRACTION = "FULL_PARTITION"
  val STREAM_EXTRACTION = "STREAM"

}
