package com.gestamp.hypatia.integration.utils

object ConstantsIntegration {

  val SOURCE_PARAM = "source"
  val DATASET_PARAM = "dataset"
  val STRING = "String"
  val INTEGER = "Integer"
  val BIGINT = "BigInt"
  val DOUBLE = "Double"
  val BOOLEAN = "Boolean"
  val TIMESTAMP = "Timestamp"
  val STRUCT = "Struct"
  val ARRAY = "Array"
  val DECIMAL_38_18 = "Decimal(38 18)"
  val DECIMAL_38_4 = "Decimal(38 4)"
  val DECIMAL_18_4 = "Decimal(18 4)"
  val DECIMAL_16_4 = "Decimal(16 4)"
  val DECIMAL_16_5 = "Decimal(16 5)"
  val SHORT = "Short"
  val LONG = "Long"
  val DATE = "Date"
  val BYTE = "Byte"
  val BINARY = "Binary"
  val FLOAT = "Float"

  val OPERATION_UNIONBYNAME = "unionbyname"
  val OPERATION_UNION = "union"
  val NO_OPERATION = "none"
  val JOIN_OPERATIONS = Seq("inner", "outer", "left", "right")
  val CUSTOM_OPERATION = "custom"

  val FULL_EXTRACTION = "FULL"
  val DELTA_EXTRACTION = "DELTA"
  val INCREMENTAL_EXTRACTION = "INCREMENTAL"
  val CDC_EXTRACTION = "CDC"
  val FULL_PARTITION_EXTRACTION = "FULL_PARTITION"
  val STREAM_EXTRACTION = "STREAM"
}