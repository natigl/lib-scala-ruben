package com.gestamp.hypatia.args

import com.gestamp.hypatia.exceptions.LakehouseParserArgsException
import com.gestamp.hypatia.utils.ConstantsLakehouse
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

object LakehouseParserArgs extends OptionParser[LakehouseJobParams]("Lakehouse actions"){

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * @param args: Argumentos de la App
   * @return
   */
  def parserArgs(args: Array[String]): Either[Throwable, LakehouseJobParams] = {
    logger.info(s"Start parseArgs method, args: ${args.mkString(", ")}")

    if (args.length == 0)
      throw new LakehouseParserArgsException("No hay parametros de entrada para la ejecución de esta librería")

    parserArgumentos.parse(args, LakehouseJobParams()) match {
      case Some(parsedArgs) =>
        Right(LakehouseJobParams(parsedArgs.tables, parsedArgs.target_layer, parsedArgs.action, parsedArgs.source, parsedArgs.schema_evolution_id, parsedArgs.delta_version))
      case _ =>
        throw new LakehouseParserArgsException(s"Los parametros introducidos no son correctos: ${args.mkString(", ")}")
    }
  }


  /**
   * Método que parsea todos los argumentos de entrada de la librería a través de la librería scopt.
   */
  private val parserArgumentos = new OptionParser[LakehouseJobParams]("Lakehouse scopt") {

    logger.info("Start Scopt parser input arguments")

    head("Ingestion Scopt version => ", "3.6.11")

    opt[String]('t', ConstantsLakehouse.TABLE_PARAM)
      .valueName(ConstantsLakehouse.TABLE_PARAM)
      .action((value, arguments) => arguments.copy(tables =
        value.replaceAll("[\\[ \\] \"]", "").split(",").toSeq))
      .text("Processed tables")
      .validate(value => if(value == "" || value == null)
        failure("Parameter tables is missing") else success)

    opt[String]('l', ConstantsLakehouse.TARGET_LAYER_PARAM)
      .valueName(ConstantsLakehouse.TARGET_LAYER_PARAM)
      .action((value, arguments) => arguments.copy(target_layer = value))
      .text("Target layer")
      .validate(value => if (value == "" || value == null || (value != ConstantsLakehouse.RAW_SOURCES_LAYER && value != ConstantsLakehouse.RAW_CLEAN_LAYER && value != ConstantsLakehouse.INTEGRATION_LAYER))
        failure(s"Parameter target_layer is missing, value: $value") else success)

    opt[String]('a', ConstantsLakehouse.ACTION_PARAM)
      .valueName(ConstantsLakehouse.ACTION_PARAM)
      .action((value, arguments) => arguments.copy(action = value))
      .text("Action to be carried out on tables")
      .validate(value => if (value == "" || value == null)
        failure("Parameter action is missing") else success)

    opt[String]('s', ConstantsLakehouse.SOURCE_PARAM)
      .valueName(ConstantsLakehouse.SOURCE_PARAM)
      .action((value, arguments) => arguments.copy(source = value))
      .text("Source of the datasets involved")
      .validate(value => if (value == "" || value == null)
        failure("Parameter source is missing") else success)

    opt[String]('i', ConstantsLakehouse.SCHEMA_EVOLUTION_ID_PARAM)
      .valueName(ConstantsLakehouse.SCHEMA_EVOLUTION_ID_PARAM)
      .action((value, arguments) =>
        arguments.copy(schema_evolution_id = if (value == "" || value == null) None else Some(value.toInt)))
      .text("Schema evolution id to carry out the action")

    opt[String]('v', ConstantsLakehouse.DELTA_VERSION_PARAM)
      .valueName(ConstantsLakehouse.DELTA_VERSION_PARAM)
      .action((value, arguments) =>
        arguments.copy(delta_version = if (value == "" || value == null) None else Some(value.toInt)))
      .text("Delta version for reloading")


    help("help")
      .text("Pintar mensaje de uso de los parametros")
  }
}
