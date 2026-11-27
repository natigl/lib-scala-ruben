package com.gestamp.hypatia.args

import com.gestamp.hypatia.exceptions.HypatiaManagerParserException
import com.gestamp.hypatia.utils.HypatiaManagerConstants
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser
import com.gestamp.hypatia.postgresql.dao.UsecaseDao

object DeltaMonitoringParserArgs extends OptionParser[JobParamsDeltaMonitoring]("DeltaMonitoringGold") {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)


  /**
   *
   * @param args
   * @return
   */
  def parserArgs(args: Array[String]): JobParamsDeltaMonitoring = {
    logger.info(s"Start parseArgs method, args: ${args.mkString(", ")}")

    if (args.isEmpty)
      throw new Exception("0 Parameters")

    parserArgumentos.parse(args, JobParamsDeltaMonitoring("", None)) match {
      case Some(parsedArgs) =>
        val usecaseDao = new UsecaseDao().getUsecaseByHyptProcess(parsedArgs.idHypatiaManagerProcess)
        parsedArgs.copy(usecase = Some(usecaseDao))
      case _ =>
        throw new HypatiaManagerParserException(s"Los parámetros introducidos no son correctos: ${args.mkString(", ")}")
    }
  }


  /**
   * Método que parsea todos los argumentos de entrda a tarvés de la librería scopt.
   */
  val parserArgumentos = new OptionParser[JobParamsDeltaMonitoring]("HypatIA Monitoring scopt") {

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
