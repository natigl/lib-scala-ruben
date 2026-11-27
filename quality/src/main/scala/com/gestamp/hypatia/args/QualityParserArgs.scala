package com.gestamp.hypatia.args

import com.gestamp.hypatia.exceptions.QualityParserArgsException
import com.gestamp.hypatia.postgresql.dao.DatasetDao
import com.gestamp.hypatia.utils.ConstantsQuality
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

object QualityParserArgs extends OptionParser[QualityJobParams]("Quality ingestion"){

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val datasetDao = new DatasetDao

  /**
   *
   * @param args
   * @return
   */
  def parserArgs(args: Array[String]): Either[Throwable, QualityJobParams] = {
    logger.info(s"Start parseArgs method, args: ${args.mkString(", ")}")

    if (args.length == 0)
      throw new QualityParserArgsException("No hay parametros de entrada para la ejecución de esta librería")

      parserArgumentos.parse(args, QualityJobParams()) match {
        case Some(parsedArgs) =>
          // La resolución de los datasets contra la BD se hace aquí, tras el parseo, y no en la acción del -d:
          // un mismo nombre de dataset puede existir en varios sources, por lo que hace falta el par
          // (nombre, source) para resolver el dataset único, y scopt procesa los argumentos en el orden de la
          // línea de comandos, así que dentro de la acción del -d el -o (source) podría no estar parseado aún.
          val datasets = datasetDao.getDatasetsBySequenceNames(parsedArgs.datasetNames, parsedArgs.source)
          logger.info(s"Resolved ${datasets.size} dataset(s) for source '${parsedArgs.source}': " +
            datasets.map(dataset => s"${dataset.dataset}#${dataset.id}(group=${dataset.group})").mkString(", "))
          val missing = parsedArgs.datasetNames.filterNot(name => datasets.exists(_.dataset == name))
          if (missing.nonEmpty)
            logger.warn(s"No active datasets found for source '${parsedArgs.source}' with names: ${missing.mkString(", ")}")
          Right(QualityJobParams(parsedArgs.source, datasets))
        case _ =>
          Left(new QualityParserArgsException(s"Los parametros introducidos no son correctos: ${args.mkString(", ")}"))
    }
  }


  /**
   * Método que parsea todos los argumentos de entrda de la librería a tarvés de la librería scopt.
   */
  val parserArgumentos = new OptionParser[QualityJobParams]("Ingestion scopt") {

    logger.info("Start Scopt parser input arguments")

    head("Ingestion Scopt version => ", "3.6.11")

    opt[String]('o', ConstantsQuality.SOURCE_PARAM)
      .valueName(ConstantsQuality.SOURCE_PARAM)
      .action((value, arguments) => arguments.copy(source = value))
      .text("Fuente origen del dataset")
      .validate(value => if(value == "" || value == null)
        failure("Falta el parametro source") else success)

    opt[String]('d', ConstantsQuality.DATASET_PARAM)
      // Solo guarda los nombres: la resolución contra la BD se hace en parserArgs, cuando ya se conoce el source
      .action((value, arguments) => arguments.copy(datasetNames =
                value.replaceAll("[\\[ \\] \"]", "").split(",").toSeq))
      .text("Json String de los datasets a ingestar en la capa Bronze del Data Lake")
      .validate(value => if (value == "" || value == null)
        failure("Falta el parametro dataset") else success)

    help("help")
      .text("Pintar mensaje de uso de los parametros")
  }

}
