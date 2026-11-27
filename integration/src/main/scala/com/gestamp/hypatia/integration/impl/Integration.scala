package com.gestamp.hypatia.integration.impl

import com.gestamp.hypatia.integration.args.IntegrationJobParams
import com.gestamp.hypatia.integration.exception.IntegrationManagerException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object Integration {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Punto de entrada del proceso de integración tras el parseo de argumentos:
   * delega en IntegrationManager.handleData y cierra el proceso.
   *
   * @param jobParams parámetros del job (source y distillations ya resueltas)
   * @return los nombres de las distillations procesadas correctamente
   * @throws IntegrationManagerException si el proceso falla
   */
  def startIntegration(jobParams: IntegrationJobParams): List[String] = {
    logger.info("Start of Integration process after parsing input args")

    Try(IntegrationManager.handleData(jobParams: IntegrationJobParams)) match {
      case Success(value) =>
        println(s"The following distillation datasets were successfully processed: ${value.mkString(", ")}")
        endIntegration()
        value
      case Failure(exception) => throw new IntegrationManagerException(
        s"Error: Integration process failed running for ${jobParams.source} source and ${jobParams.distillations.map(d => s""""${d.dataset_name}"""").mkString(", ")} datasets.\n " +
        s"Exception message: ${exception.getMessage}.\n ",
        exception)
    }
  }

  /**
   * Cierra el proceso de integración (log + tareas finales del manager).
   *
   * @return mensaje de fin del proceso
   */
  private def endIntegration(): String = {
    logger.info("Integration process ended")
    IntegrationManager.finishTasksIntegration()
  }

}
