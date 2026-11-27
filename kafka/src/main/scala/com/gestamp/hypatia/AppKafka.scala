package com.gestamp.hypatia

import com.gestamp.hypatia.args.KafkaParserArgs
import com.gestamp.hypatia.exceptions.KafkaParserArgsException
import com.gestamp.hypatia.impl.Kafka.startIngestion
import org.slf4j.{Logger, LoggerFactory}

object AppKafka {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Main del job de ingesta Kafka en Databricks: parsea los argumentos y lanza la ingesta.
   *
   * @param args argumentos de línea de comandos (-o source, -d dataset)
   */
  def main(args: Array[String]): Unit = {
    logger.info(s"Start Kafka Ingestion with args = ${args.mkString(",")}")

    KafkaParserArgs.parserArgs(args) match {
      case Right(value) => startIngestion(value)
      case Left(exception) => throw new KafkaParserArgsException(exception.getMessage, exception)
    }
  }

}
