package com.gestamp.hypatia

import com.gestamp.hypatia.args.{HypatiaManagerParserArgs, JobParamsCase}
import com.gestamp.hypatia.exceptions.HypatiaManagerException
import com.gestamp.hypatia.spark.SparkOptions
import com.gestamp.hypatia.utils.CommonsConstants.ENV_SOURCE
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

abstract class HypatiaDataset(args: Array[String]) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val jobParams: JobParamsCase = HypatiaManagerParserArgs.parserArgs(args)

  /** ADLS Gold Layer Path **/
  val goldPath: String = HypatiaDatasetManager.generatePathGoldLayer(jobParams)


  /**
   * Start HypatIA Dataset Manager method
   *
   * @return
   */
  def startProcess(): Unit = {
    logger.info(s"Start HypatIA Dataset Manager: ${jobParams.idHypatiaManagerProcess}")

    Try(propagateToGold()) match {
      case Success(returnValue) => {
        logger.info(s"End Generate Model Data => $returnValue")
        endHyptManager()
      }
      case Failure(exception) => {
        throw new HypatiaManagerException(s"Error to build Gold layer databases name", exception)
      }
    }
  }


  /**
   * Method that starts the propagation of datasets from Silver Layer to Gold Layer.
   *
   * @return
   */
  def propagateToGold(): Unit


  /**
   * Finish method of HypatIA Dataset Manager
   *
   * @return
   */
  private def endHyptManager(): Unit = {
    logger.info(s"¡¡Ccongratulations!!! \n " +
      s"HyptManager '${jobParams.idHypatiaManagerProcess}' has successfully completed")
  }


  /**
   * Reads data from a Delta table in ADLS Gen2 and returns a DataFrame.
   *
   * @param catalog  The catalog (e.g., storage account and container name) where the Delta table is stored.
   * @param schema   The schema of the Delta table.
   * @param deltaObj The name of the Delta object (e.g., file or directory) to read.
   * @return A DataFrame containing the data from the specified Delta object.
   * @throws HypatiaManagerException if there is an error reading the data.
   */
  def readDataFromDeltaTable(tableName: String, deltaSpace: (String, String)): DataFrame = {
    logger.info(s"Reading data from Delta Lake: ${deltaSpace._1}.${deltaSpace._2}.$tableName")

    val catalog = deltaSpace._1
    val schema = deltaSpace._2

    Try(HypatiaDatasetManager.readDataFromDeltaTable(catalog, schema, tableName)) match {
      case Success(df) => df
      case Failure(exception) => {
        throw new HypatiaManagerException(s"Error reading Data Lake on path: $catalog, $schema, $tableName.", exception)
      }
    }
  }


  /**
   * ADLS dataframe writing method
   *
   * @param datasetName
   * @param dataFrame
   * @param path
   * @param sparkOptions
   * @return
   */
  def writeDataToDeltaTable(tableName: String, dataFrame: DataFrame, path: String, sparkOptions: SparkOptions): Unit = {
    logger.info(s"Writing data to Azure Data Lake Gen2, path = $path, saveMode = ${sparkOptions.saveMode}")

    val catalog = s"${jobParams.use_case.area.toLowerCase}$ENV_SOURCE"
    val schema = jobParams.use_case.name

    Try(HypatiaDatasetManager.writeDataToDeltaTable(s"$catalog.$schema", tableName,
      dataFrame, path + "/" + tableName, sparkOptions)) match {
      case Success(value) => logger.info(s"Delta dataset is created successfully to location: $path")
      case Failure(exception) => {
        throw new HypatiaManagerException(s"Error writing Data Lake on path: $path.", exception)
      }
    }
  }

}
