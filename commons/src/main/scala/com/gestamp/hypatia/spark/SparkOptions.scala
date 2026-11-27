package com.gestamp.hypatia.spark

import com.gestamp.hypatia.utils.CommonsConstants
import org.apache.spark.sql.SaveMode

/**
 *
 * @param fileFormat
 * @param saveMode
 * @param partitionBy
 * @param otherOptions
 */
case class SparkOptions(fileFormat: String = CommonsConstants.PARQUET_FORAMT,
                        saveMode: SaveMode = SaveMode.Overwrite,
                        outputMode: String = "",
                        partitionBy: List[String] = List.empty,
                        otherOptions: Map[String, String] = Map())
