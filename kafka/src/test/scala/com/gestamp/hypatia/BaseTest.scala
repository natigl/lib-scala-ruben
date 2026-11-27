package com.gestamp.hypatia

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._

trait BaseTest extends FunSuite with Matchers {

  lazy val spark: SparkSession = SparkSession.builder().master("local").appName("Test Session").getOrCreate()

  def equalDataFrames(expected: DataFrame, result: DataFrame): Unit = {
    expected.schema should equal(result.schema)
    expected.collect() should contain theSameElementsAs result.collect()
  }
}