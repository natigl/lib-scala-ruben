package com.gestamp.hypatia.postgresql.dao

import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

class HyptManagerDaoTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /*test("Get HypatIA Dataset Manager by hypt_process") {
    val hmd = new HypatiaDatasetManagerDao
    val result = hmd.getHyptManagerByProcess("HR_Analytics#Maestros")
    logger.info(s"HyptManager result = ${result.custom_params.get("source")}")

    assert(result != null)
  }*/

}
