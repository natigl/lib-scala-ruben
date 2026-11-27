package com.gestamp.hypatia.args

import com.gestamp.hypatia.exceptions.HypatiaManagerParserException
import com.gestamp.hypatia.postgresql.dao.{HypatiaDatasetManagerDao, UsecaseDao}
import com.gestamp.hypatia.utils.{HypatiaManagerConstants}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser


object HypatiaManagerParserArgs extends OptionParser[JobParamsCase]("HypatiaDatasetManager") {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val uso = """
    Usage: use-case-artifact-jar-with-dependencies.jar [-p IdUseCaseProcess]
  """

  /**
   *
   * @param args
   * @return
   */
  def parserArgs(args: Array[String]): JobParamsCase = {
    logger.info(s"Start parseArgs method, args: ${args.mkString(", ")}")

    if (args.length == 0)
      throw new Exception(uso)

    parserArgumentos.parse(args, JobParamsCase()) match {
      case Some(parsedArgs) => {
        val hyptManagerParams = new HypatiaDatasetManagerDao().getHyptManagerByProcess(parsedArgs.idHypatiaManagerProcess)
        val use_case = new UsecaseDao().getUsecaseByHyptProcess(parsedArgs.idHypatiaManagerProcess)

        JobParamsCase(parsedArgs.idHypatiaManagerProcess, hyptManagerParams.custom_params,use_case)
      }
      case _ =>
        throw new HypatiaManagerParserException(s"Los parametros introducidos no son correctos: ${args.mkString(", ")}")
    }

  }


  /**
   * Método que parsea todos los argumentos de entrda a tarvés de la librería scopt.
   */
  val parserArgumentos = new OptionParser[JobParamsCase]("HypatIA Dataset Manager scopt") {

    logger.info("Start Scopt parser input arguments")

    head("HypatiaManager Scopt version => ", "3.7.1")

    opt[String]('p', HypatiaManagerConstants.ID_HYPATIA_PROCESS_PARAM)
      .valueName(HypatiaManagerConstants.ID_HYPATIA_PROCESS_PARAM)
      .action((value, arguments) => arguments.copy(idHypatiaManagerProcess = value))
      .text("Identificador del proceso")
      .validate(value => if(value == "" || value == null)
        failure("Falta el parametro 'hypt_process'") else success)

    help("help")
      .text("Pintar mensaje de uso de los parametros")
  }

}
