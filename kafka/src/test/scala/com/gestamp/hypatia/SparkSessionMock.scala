package com.gestamp.hypatia

import com.gestamp.hypatia.spark.SparkConfiguration
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

trait SparkSessionMock extends MockitoSugar {

  def mockSparkContext(): SparkSession = {

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
