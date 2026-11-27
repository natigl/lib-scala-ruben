package com.gestamp.hypatia.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.scalatest.{FunSuite, Matchers}

trait SparkConfigurationTest extends FunSuite with Matchers with MockitoSugar {

  def mockedSparkContext(): SparkSession = {
    val sparkSession: SparkSession = mock[SparkSession]
    val sparkConfiguration: SparkConfiguration = mock[SparkConfiguration]
    val df: DataFrame = mock[DataFrame]
    val reader: DataFrameReader = mock[DataFrameReader]

    when(sparkConfiguration.spark).thenReturn(sparkSession)
    when(sparkSession.read).thenReturn(reader)
    when(reader.format(ArgumentMatchers.anyString())).thenReturn(reader)
    when(reader.load(ArgumentMatchers.anyString())).thenReturn(df)

    sparkSession
  }

}
