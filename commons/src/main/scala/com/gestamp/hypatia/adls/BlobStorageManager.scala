package com.gestamp.hypatia.adls

import com.gestamp.hypatia.exceptions.BlobStorageException
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object BlobStorageManager {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Reads a DataFrame from Azure Blob Storage using the specified options.
   *
   * @param path         Path in Azure Blob Storage from which the data will be read.
   * @param sparkOptions Spark options for reading the DataFrame.
   * @return A DataFrame with the data read from Azure Blob Storage.
   * @throws BlobStorageException If the path doesn't exist, or an error occurs while reading the data from Blob Storage.
   */
  @throws(classOf[BlobStorageException])
  def readCsv(path: String, sparkOptions: SparkOptions): DataFrame = {
    logger.info(s"Start reading csv from Azure Blob Storage at path: $path")

    // Resolve via SparkConfiguration.spark (SparkSessionHolder -> active -> default -> getOrCreate())
    // instead of SparkSession.builder().getOrCreate() directly: on chained Databricks tasks sharing
    // the same cluster/JVM (e.g. Ingestion -> Quality -> Integration), a bare getOrCreate() can return
    // a stale session left over from a previous task, with ADLS credentials/config scoped to a
    // different container/path than the one being read here.
    val spark = SparkConfiguration.spark

    val reader = spark.read.format(sparkOptions.fileFormat)

    if (sparkOptions.otherOptions.nonEmpty) {
      logger.info(s"Other reader options: ${sparkOptions.otherOptions.toString()}")
      reader.options(sparkOptions.otherOptions)
    }

    Try(reader.load(path)) match {
      case Success(df) => df
      case Failure(e) =>
        throw new BlobStorageException(s"Error reading Csv from Azure Blob Storage at path_csv: $path", e)
    }
  }
}
