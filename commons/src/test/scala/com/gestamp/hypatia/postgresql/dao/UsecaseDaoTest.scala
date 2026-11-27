package com.gestamp.hypatia.postgresql.dao

import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

class UsecaseDaoTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /*test("Get Usecase by id") {
    val ucd = new UsecaseDao
    val result = ucd.getUsecaseByUsecaseId(1)
    logger.info(s"Usecase result = ${result.name}")

    assert(result != null)
  }

  test("Get Usecase by area") {
    val ucd = new UsecaseDao
    val result = ucd.getUsecaseByArea("HR_Analytics")
    logger.info(s"Usecase result size: ${result.size}")

    assert(result != null)
  }

  test("Get Usecase by program") {
    val ucd = new UsecaseDao
    val result = ucd.getUsecaseByProgram("Atenea")
    logger.info(s"Usecase result size: ${result.size}")

    assert(result != null)
  }*/

}
