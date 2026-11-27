package com.gestamp.hypatia.impl

import com.gestamp.hypatia.args.HistoricalQualityJobParams
import com.gestamp.hypatia.exceptions.HistoricalQualityManagerException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object HistoricalQuality {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Punto de entrada del proceso de calidad histórico: delega en
   * HistoricalQualityManager.handleData y cierra el proceso.
   *
   * @param jobParams parámetros del job histórico
   * @throws HistoricalQualityManagerException si el proceso falla
   */
  def startHistoricalQuality(jobParams: HistoricalQualityJobParams): Unit = {
    logger.info("Start Historical Quality")

    Try(HistoricalQualityManager.handleData(jobParams)) match {
      case Success(value) => endHistoricalQuality()
      case Failure(exception) => throw new HistoricalQualityManagerException("Error in handleData historical Quality method ", exception)
    }
  }

  /**
   * Cierra el proceso de calidad histórico (log + tareas finales del manager).
   *
   * @return mensaje de fin del proceso
   */
  private def endHistoricalQuality(): String = {
    logger.info("End Historical Quality")

    Try(HistoricalQualityManager.finishTasksHistoricalQuality()) match {
      case Success(value) => value
      case Failure(exception) => throw new HistoricalQualityManagerException("Error finishing historical quality tasks", exception)
    }
  }

}
