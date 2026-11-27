package com.gestamp.hypatia.impl

import com.gestamp.hypatia.args.HistoricalIngestionJobParams
import com.gestamp.hypatia.exceptions.IngestionManagerException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object HistoricalIngestion {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Método principal que orquesta la ingesta de todo el historico de ficheros con el Autoloader
   *
   * @param jobParams
   */
  def startHistoricalIngestion(jobParams: HistoricalIngestionJobParams): Unit = {
    logger.info("Start Historical Ingestion after parser input args")

    Try(HistoricalIngestionManager.handleData(jobParams)) match {
      case Success(value) => endHistoricalIngestion()
      case Failure(exception) =>
        throw new IngestionManagerException("Error in handleData Historical Ingestion method", exception)
    }
  }

  /**
   * Método de finalización de la ingesta historica
   *
   * @return
   */
  private def endHistoricalIngestion(): String = {
    logger.info("End Method Historical Ingestion")

    Try(HistoricalIngestionManager.finishTasksHistoricalIngestion()) match {
      case Success(value) => value
      case Failure(exception) =>
        throw new IngestionManagerException("Error finishing historical ingestion tasks", exception)
    }
  }

}
