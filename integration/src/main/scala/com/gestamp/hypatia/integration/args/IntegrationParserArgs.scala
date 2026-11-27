package com.gestamp.hypatia.integration.args

import com.gestamp.hypatia.integration.exception.IntegrationParserArgsException
import com.gestamp.hypatia.integration.utils.ConstantsIntegration
import com.gestamp.hypatia.postgresql.dao.DataRelationshipDao
import com.gestamp.hypatia.postgresql.model.DataRelationship
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

object IntegrationParserArgs extends OptionParser[IntegrationJobParams]("Integration ingestion "){

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val datarelationshipDao = new DataRelationshipDao

  /**
   *
   * @param args
   * @return
   */
  def parserArgs(args: Array[String]): Either[Throwable, IntegrationJobParams] = {
    logger.info(s"Start parseArgs method, args: ${args.mkString(", ")}")

    if (args.length == 0)
      throw new IntegrationParserArgsException("No hay parametros de entrada para la ejecución de esta librería")

      parserArgumentos.parse(args, IntegrationJobParams()) match {
        case Some(parsedArgs) =>
          // La resolución de las distillations contra la BD se hace aquí, tras el parseo, y no en la acción del -d:
          // un mismo nombre de dataset puede existir en varios sources, por lo que hace falta el par
          // (nombre, source) para resolver el dataset único, y scopt procesa los argumentos en el orden de la
          // línea de comandos, así que dentro de la acción del -d el -o (source) podría no estar parseado aún.
          val distillations = datarelationshipDao.getDataRelationshipByDatasetName(parsedArgs.datasetNames, parsedArgs.source)
          logger.info(s"Resolved ${distillations.size} data relationship(s) for source '${parsedArgs.source}': " +
            distillations.map(dr => s"${dr.dataset_name}#${dr.dataset_id} -> ${dr.distillation_name}").mkString(", "))
          val missing = parsedArgs.datasetNames.filterNot(name => distillations.exists(_.dataset_name == name))
          if (missing.nonEmpty)
            logger.warn(s"No data relationships found for source '${parsedArgs.source}' with dataset names: ${missing.mkString(", ")}")
          Right(IntegrationJobParams(parsedArgs.source, distillations))
        case _ =>
          Left(new IntegrationParserArgsException(s"Los parametros introducidos no son correctos: ${args.mkString(", ")}"))
    }
  }


  /**
   * Método que parsea todos los argumentos de entrada de la librería a tarvés de la librería scopt.
   */
  val parserArgumentos = new OptionParser[IntegrationJobParams]("Ingestion scopt") {

    logger.info("Start Scopt parser input arguments")

    head("Ingestion Scopt version => ", "3.6.11")

    opt[String]('o', ConstantsIntegration.SOURCE_PARAM)
      .valueName(ConstantsIntegration.SOURCE_PARAM)
      .action((value, arguments) => arguments.copy(source = value))
      .text("Fuente origen del dataset")
      .validate(value => if(value == "" || value == null)
        failure("Falta el parametro source") else success)

    opt[String]('d', ConstantsIntegration.DATASET_PARAM)
      // Solo guarda los nombres: la resolución contra la BD se hace en parserArgs, cuando ya se conoce el source
      .action((value, arguments) => arguments.copy(datasetNames =
        value.replaceAll("[\\[ \\] \"]", "").split(",").toSeq))
      .text("Distillations a procesar en la capa Silver del Data Lake")
      .validate(value => if (value == "" || value == null)
        failure("Falta el parametro dataset") else success)

    help("help")
      .text("Pintar mensaje de uso de los parametros")
  }

}
