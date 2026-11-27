package com.gestamp.hypatia.args

import com.gestamp.hypatia.exceptions.KafkaParserArgsException
import com.gestamp.hypatia.postgresql.dao.DatasetDao
import com.gestamp.hypatia.utils.ConstantsKafka
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

object KafkaParserArgs extends OptionParser[KafkaJobParams]("Ingestion Kafka"){

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val datasetDao = new DatasetDao

  /**
   *
   * @param args
   * @return
   */
  def parserArgs(args: Array[String]): Either[Throwable, KafkaJobParams] = {
    logger.info(s"Start parseArgs method, args: ${args.mkString(", ")}")

    if (args.length == 0)
      throw new KafkaParserArgsException("No hay parametros de entrada para la ejecución de esta librería")

      parserArgumentos.parse(args, KafkaJobParams()) match {
        case Some(parsedArgs) =>
          // La resolución del dataset contra la BD se hace aquí, tras el parseo, y no en la acción del -d:
          // un mismo nombre de dataset puede existir en varios sources, por lo que hace falta el par
          // (nombre, source) para resolver el dataset único, y scopt procesa los argumentos en el orden de la
          // línea de comandos, así que dentro de la acción del -d el -o (source) podría no estar parseado aún.
          val dataset = datasetDao.getDatasetByName(parsedArgs.datasetName, parsedArgs.source)
          logger.info(s"Resolved dataset for source '${parsedArgs.source}': " +
            s"${dataset.dataset}#${dataset.id}(group=${dataset.group})")
          Right(KafkaJobParams(parsedArgs.source, dataset))
        case _ =>
          throw new KafkaParserArgsException(s"Los parametros introducidos no son correctos: ${args.mkString(", ")}")
    }
  }


  /**
   * Método que parsea todos los argumentos de entrda de la librería a tarvés de la librería scopt.
   */
  val parserArgumentos = new OptionParser[KafkaJobParams]("Ingestion scopt") {

    logger.info("Start Scopt parser input arguments")

    head("Ingestion Scopt version => ", "3.6.11")

    opt[String]('o', ConstantsKafka.SOURCE_PARAM)
      .valueName(ConstantsKafka.SOURCE_PARAM)
      .action((value, arguments) => arguments.copy(source = value))
      .text("Fuente origen del dataset")
      .validate(value => if(value == "" || value == null)
        failure("Falta el parametro source") else success)

    opt[String]('d', ConstantsKafka.DATASET_PARAM)
      // Solo guarda el nombre: la resolución contra la BD se hace en parserArgs, cuando ya se conoce el source
      .action((value, arguments) => arguments.copy(datasetName = value))
      .text("Json String de los datasets a ingestar en la capa Bronze del Data Lake")
      .validate(value => if (value == "" || value == null)
        failure("Falta el parametro dataset") else success)

    help("help")
      .text("Pintar mensaje de uso de los parametros")
  }

}
