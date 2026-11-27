package com.gestamp.hypatia.postgresql.dao

import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

class DevelopmentCaseDaoTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /*test("Get DevelopmentCase by usecase_id") {
    val dcd = new DevelopmentCaseDao
    val result = dcd.getDevelopmentaseByUsecaseId(1)
    logger.info(s"DevelopmentCase result = ${result.hypt_process}")

    assert(result != null)
  }

  test("Get DevelopmentCase by hypt_process") {
    val dcd = new DevelopmentCaseDao
    val result = dcd.getDevelopmentCaseByHyptProcess("HR_Analytics#Maestros")
    logger.info(s"DevelopmentCase result size= ${result.size}")

    assert(result != null)
  }

  test("Get DevelopmentCase by distillation_id") {
    val dcd = new DevelopmentCaseDao
    val result = dcd.getDevelopmentCaseByDistillationId(11)
    logger.info(s"DevelopmentCase result size= ${result.hypt_process}")

    assert(result != null)
  }*/

}
