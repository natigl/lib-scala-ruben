package com.gestamp.hypatia.utils

import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

object Utils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Elimina del DataFrame las columnas técnicas del Change Data Feed
   * (_change_type, _commit_version, _commit_timestamp, _rescued_data) si existen.
   *
   * @param df DataFrame de entrada
   * @return el DataFrame sin las columnas CDF
   */
  def dropCDFcolumns(df: DataFrame): DataFrame = {
    logger.info("Delete columns CDF")
    val columnsDrop = Seq("_change_type", "_commit_version", "_commit_timestamp", "_rescued_data")

    columnsDrop.foreach(col =>
      if (df.columns.contains(col)) {
        logger.info(s"This dataframe contain CDF column $col")
        df.drop(col)
        logger.info(s"CDF column $col deleted")
      })
    logger.info(s"CDF columns deleted in dataframe")
    df
  }
}
