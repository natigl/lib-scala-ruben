package com.gestamp.hypatia.impl

import com.gestamp.hypatia.args.IngestionJobParams
import com.gestamp.hypatia.exceptions.IngestionManagerException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object IngestionLogic {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)


  /**
   * Método principal que orquesta la ingesta de los nuevos ficheros con el AutoLoader
   * @param jobParams Input Ingestion String Args
   */
  def startIngestion(jobParams: IngestionJobParams): List[String] = {
    logger.info("Start of Ingestion process after parsing input args")
    Try(IngestionManager.handleData(jobParams)) match {
      case Success(value) =>
        println(s"The following datasets were successfully processed: ${value.mkString(", ")}")
        endIngestion()
        value
      case Failure(exception) => throw new IngestionManagerException(
        s"Error: Ingestion process failed running for ${jobParams.source} source and ${jobParams.datasets.toString} datasets.\n " +
        s"Exception message: ${exception.getMessage}.\n ",
        exception)
    }
  }

  /**
   * Método de finalización del AutoLoader
   * @return
   */
  private def endIngestion(): String = {
    logger.info("Ingestion process ended")
    IngestionManager.finishTasksIngestion()
  }

}
