package com.gestamp.hypatia.utils

import com.gestamp.hypatia.spark.SparkConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql._
import org.mockito.{ArgumentMatchers, MockitoSugar}

class IngestionUtilsTest extends FunSuite with MockitoSugar {

  /*implicit val spark = SparkConfiguration.spark
  import spark.implicits._

  val df = Seq(
    (1, "2020"),
    (2, "2020"),
    (3, "2020")
  ).toDF("appId", "year")*/

}