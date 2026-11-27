package com.gestamp.hypatia.impl

import com.gestamp.hypatia.args.QualityJobParams
import com.gestamp.hypatia.exceptions.QualityManagerException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object Quality {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Punto de entrada del proceso de calidad tras el parseo de argumentos:
   * delega en QualityManager.handleData y cierra el proceso.
   *
   * @param jobParams parámetros del job (source y datasets ya resueltos)
   * @return los nombres de los datasets procesados correctamente
   * @throws QualityManagerException si el proceso de calidad falla
   */
  def startQualityIngestion(jobParams: QualityJobParams): List[String] = {
    logger.info("Start of Quality process after parsing input args")

    Try(QualityManager.handleData(jobParams)) match {
      case Success(value) =>
        println(s"The following datasets were successfully processed: ${value.mkString(", ")}")
        endQualityIngestion()
        value
      case Failure(exception) => throw new QualityManagerException(
        s"Error: Quality process failed running for ${jobParams.source} source and ${jobParams.datasets.toString} datasets.\n " +
        s"Exception message: ${exception.getMessage}.\n",
        exception)
    }
  }

  /**
   * Cierra el proceso de calidad (log + tareas finales del manager).
   *
   * @return mensaje de fin del proceso
   */
  private def endQualityIngestion(): String = {
    logger.info("Quality process ended")
    QualityManager.finishTasksQuality()
    }

}
