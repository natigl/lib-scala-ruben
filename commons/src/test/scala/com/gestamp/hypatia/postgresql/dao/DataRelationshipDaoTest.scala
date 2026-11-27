package com.gestamp.hypatia.postgresql.dao

import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

class DataRelationshipDaoTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val filed_compare = Seq("ACTIONTYPE","ZACTREAS")

  private val JOIN_OPERATIONS = Seq("inner", "outer", "left", "right")

  /*test("Get Data Relationship by Distillation Id") {
    val dr = new DataRelationshipDao
    val result = dr.getDataRelationshipByDistillationId(20)
    logger.info(s"Dataset Relationship size result: ${result.size}")

    val fields = result.head.fields.split(",").map(_.trim).toSeq
    assert(result != null)
  }

  test("compary seqs") {
    val operation = "inner"

    operation match {
      case x if JOIN_OPERATIONS.contains(x) => logger.info("Está!!!")
      case _ => logger.info("No Está!!!")
    }
  }*/

}
