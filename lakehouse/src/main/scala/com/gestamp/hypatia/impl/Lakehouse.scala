package com.gestamp.hypatia.impl

import com.gestamp.hypatia.args.LakehouseJobParams
import com.gestamp.hypatia.exceptions.LakehouseManagerException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object Lakehouse {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Punto de entrada de las acciones de lakehouse tras el parseo de argumentos:
   * delega en LakehouseManager.handleData y cierra el proceso.
   *
   * @param jobParams parámetros del job (tablas, capa, acción...)
   * @throws LakehouseManagerException si alguna acción falla
   */
  def startLakehouseActions(jobParams: LakehouseJobParams): Unit = {
    logger.info("Start lakehouse actions after parsing input args")

    Try(LakehouseManager.handleData(jobParams)) match {
      case Success(value) => endLakehouseActions()
      case Failure(exception) => throw new LakehouseManagerException("Error in lakehouse actions handleData method", exception)
    }
  }

  /**
   * Cierra el proceso de acciones de lakehouse (log + tareas finales del manager).
   *
   * @return mensaje de fin del proceso
   */
  private def endLakehouseActions(): String = {
    logger.info("End lakehouse actions")

    Try(LakehouseManager.finishLakehouseActions()) match {
      case Success(value) => value
      case Failure(exception) => throw new LakehouseManagerException("Error finishing lakehouse actions", exception)
    }
  }
}

