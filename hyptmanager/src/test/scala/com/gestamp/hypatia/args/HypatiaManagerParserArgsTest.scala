package com.gestamp.hypatia.args

import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

class HypatiaManagerParserArgsTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val inputDatasetsId = "[3,6]"
  val args = Array("-p", "GMS_File_System#GMS Group", "-d", inputDatasetsId)

  val args_FAIL = Array("-o", "BW_Human_Resources", "-d", inputDatasetsId)

 /* test("Testing HypatIA Dataset Manager parser input parameter OK") {
    val result: JobParamsCase = HypatiaManagerParserArgs.parserArgs(args)

    logger.info(s"Result of parser: ${result.idHypatiaManagerProcess}")

  }

  test("Testing HypatIA Dataset Manager parser input parameter FAIL") {

  }*/

}
