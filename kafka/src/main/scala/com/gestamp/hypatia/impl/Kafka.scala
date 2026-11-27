package com.gestamp.hypatia.impl

import com.gestamp.hypatia.args.KafkaJobParams
import com.gestamp.hypatia.exceptions.KafkaManagerException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object Kafka {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Punto de entrada de la ingesta Kafka tras el parseo de argumentos:
   * delega en KafkaManager.handleData y cierra el proceso.
   *
   * @param jobParams parámetros del job (source y dataset ya resuelto)
   * @throws KafkaManagerException si la ingesta falla
   */
  def startIngestion(jobParams: KafkaJobParams): Unit = {
    logger.info("Start Method Kafka after parser input args")

    Try(KafkaManager.handleData(jobParams)) match {
      case Success(value) => endIngestion()
      case Failure(exception) => throw new KafkaManagerException("Error in handleData method Kafka", exception)
    }
  }

  /**
   * Cierra el proceso de ingesta Kafka (log + tareas finales del manager).
   *
   * @return mensaje de fin del proceso
   */
  private def endIngestion(): String = {
    logger.info("End Method Ingestion Kafka")

    Try(KafkaManager.finishTasksKafka()) match {
      case Success(value) => value
      case Failure(exception) => throw new KafkaManagerException("Error finish Kafka tasks", exception)
    }
  }

}
