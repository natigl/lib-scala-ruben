package com.gestamp.hypatia.utils

import com.gestamp.hypatia.SparkSessionMock
import com.gestamp.hypatia.spark.SparkConfiguration
import com.gestamp.hypatia.integration.utils.SchemaCsvFileReader.castDataFrameByCSVSchema
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

class SchemaCsvFileReaderTest extends FunSuite with SparkSessionMock {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  mockSparkContext()
  val spark = SparkConfiguration.spark

  import spark.implicits._

  //Definimos el esquema con diferentes tipos de datos
  val schema = StructType(Array(
    StructField("int_col", StringType, true),
    StructField("long_col", StringType, true),
    StructField("float_col", StringType, true),
    StructField("double_col", StringType, true),
    StructField("decimal_col", StringType, true),
    StructField("string_col", StringType, true),
    StructField("binary_col", StringType, true),
    StructField("boolean_col", StringType, true),
    StructField("date_col", StringType, true),
    StructField("timestamp_col", StringType, true),
    //StructField("array_col", ArrayType(IntegerType, true), true),
    //StructField("map_col", MapType(StringType, IntegerType, true), true),
    //StructField("struct_col", StructType(Seq(StructField("inner_col", IntegerType, true))), true)
  ))

  test("Test castDataFrameByCSVSchema - Returns a DataFrame with correct schema") {
    val source = "bw_human_resources"
    val tableDistillation = "gender"

    val dataExample = Seq(
      ("1", "10L", "1.5f", "2.5", BigDecimal("100.00").toString(), "foo", "BinaryData".getBytes.toString, "true", java.sql.Date.valueOf("2022-01-01").toString, java.sql.Timestamp.valueOf("2022-01-01 00:00:00").toString),
      ("1", "10L", "1.5f", "2.5", BigDecimal("100.00").toString(), "foo", "BinaryData".getBytes.toString, "true", java.sql.Date.valueOf("2022-01-01").toString, java.sql.Timestamp.valueOf("2022-01-01 00:00:00").toString)
    )

    val dfRawClean = spark.createDataFrame(dataExample).toDF(schema.fieldNames: _*)

    val dfExample = castDataFrameByCSVSchema(source, tableDistillation, dfRawClean)
    dfExample
  }

}
