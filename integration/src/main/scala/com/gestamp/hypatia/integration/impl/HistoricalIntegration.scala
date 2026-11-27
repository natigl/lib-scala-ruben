package com.gestamp.hypatia.integration.impl

import com.gestamp.hypatia.integration.args.HistoricalIntegrationJobParams
import com.gestamp.hypatia.integration.exception.IntegrationManagerException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object HistoricalIntegration {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Punto de entrada del proceso de integración histórico: delega en
   * HistoricalIntegrationManager.handleData y cierra el proceso.
   *
   * @param jobParams parámetros del job histórico
   * @throws IntegrationManagerException si el proceso falla
   */
  def startHistoricalIntegration(jobParams: HistoricalIntegrationJobParams): Unit = {
    logger.info("Start Historical Integration")

    Try(HistoricalIntegrationManager.handleData(jobParams: HistoricalIntegrationJobParams)) match {
      case Success(value) => endHistoricalIntegration()
      case Failure(exception) => throw new IntegrationManagerException("Error in handleData Historical Integration method ", exception)
    }
  }

  /**
   * Cierra el proceso de integración histórico (log + tareas finales del manager).
   *
   * @return mensaje de fin del proceso
   */
  private def endHistoricalIntegration(): String = {
    logger.info("End of historical Integration")

    Try(HistoricalIntegrationManager.finishTasksHistoricalIntegration()) match {
      case Success(value) => value
      case Failure(exception) => throw new IntegrationManagerException("Error finishing Historical Integration tasks", exception)
    }
  }

}
