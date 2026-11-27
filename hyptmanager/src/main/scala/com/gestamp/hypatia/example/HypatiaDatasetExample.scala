package com.gestamp.hypatia.example

import com.gestamp.hypatia.HypatiaDataset
import com.gestamp.hypatia.args.JobParamsCase
import com.gestamp.hypatia.postgresql.dao.DataRelationshipDao
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import com.gestamp.hypatia.utils.CommonsConstants
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/*
class HypatiaDatasetExample(args: Array[String]) extends HypatiaDataset(args: Array[String]) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite)

  def propagateToGold(): Unit = {
    logger.info(s"Start custom propagate dataset to Gold Layer")

    readDataFromDeltaTable("job", marketplace("job"))

    "Finish process to generate serving model"
  }
}



object HypatiaDatasetExampleForDB {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Start Example")
    val hyptm = new HypatiaDatasetExample(args)
    Try(hyptm.startProcess()) match {
      case Success(value) => logger.info(s"La ejecución del HypatiaDataset ha ido correctamente: \n $value")
      case Failure(exception) => {
        logger.error(exception.getMessage)
        throw exception
      }
    }
  }

}

 */
