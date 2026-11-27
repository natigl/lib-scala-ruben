package com.gestamp.hypatia.postgresql.dao

import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

class DistillationDaoTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /*test("Get Distillation by id") {
    val dd = new DistillationDao
    val result = dd.getDistillationById(1)
    logger.info(s"Distillation result = ${result.datum}")

    assert(result != null)
  }

  test("Get Distillation by name") {
    val dd = new DistillationDao
    val result = dd.getDistillationByName("age_range")
    logger.info(s"Distillation result = ${result.id}")

    assert(result != null)
  }

  test("Get Distillations by Sequence of ids") {
    val ids: Seq[Int] = Seq(1, 2)

    val dd = new DistillationDao
    val result = dd.getDistillationBySequenceIds(ids)
    logger.info(s"Distillation result size: ${result.size}")

    assert(result != null)
  }

  test("Get Distillations by Hypt Process") {
    val dd = new DistillationDao
    val result = dd.getDistillationByHyptProcess("HR_Analytics#Maestros")
    logger.info(s"Distillation result size: ${result.size}")

    assert(result != null)
  }*/

}
