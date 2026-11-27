package com.gestamp.hypatia

import com.gestamp.hypatia.args.LakehouseParserArgs
import com.gestamp.hypatia.exceptions.LakehouseParserArgsException
import com.gestamp.hypatia.impl.Lakehouse.startLakehouseActions
import org.slf4j.{Logger, LoggerFactory}

object AppLakehouse {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Main del job de lakehouse en Databricks: parsea los argumentos y lanza las acciones.
   *
   * @param args argumentos de línea de comandos (tablas, capa, acción, source...)
   */
  def main(args: Array[String]): Unit = {
    logger.info(s"Start Lakehouse module actions with args = ${args.mkString(",")}")

    LakehouseParserArgs.parserArgs(args) match {
      case Right(value) => startLakehouseActions(value)
      case Left(exception) => throw new LakehouseParserArgsException(exception.getMessage, exception)
    }
  }
}
