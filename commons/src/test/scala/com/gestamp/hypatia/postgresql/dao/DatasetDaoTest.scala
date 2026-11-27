package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.model.Dataset
import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}


class DatasetDaoTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val expectedDatasets: Seq[Dataset] = Seq(
      Dataset(id = 3, dataset = "MAGE_RANGE", extract_type = "FULL",
              delta_column = "", format = "csv", blocksizeinmb = 100, mappings = "", header = true, trigger_id = 2,
                active = true,  group = "Maestros", delimiter = ",", key_columns = ""),
      Dataset(id = 6, dataset = "MZACTREAS", extract_type = "FULL",
              delta_column = "", format = "csv", blocksizeinmb = 100, mappings = "", header = true, trigger_id = 2,
              active = true,  group = "Maestros", delimiter = ",", key_columns = ""))

  /*test("Get Dataset by id") {
    val dd = new DatasetDao
    val result = dd.getDatasetById(20)
    logger.info(s"Dataset Size: ${result.dataset}")

    assert(result != null)
  }

  test("Get Datasets by Sequence of ids") {
    val ids: Seq[Int] = Seq(22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39)
    //[22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39]

    val dd = new DatasetDao
    val result = dd.getDatasetsBySequenceIds(ids)
    logger.info(s"Dataset size: ${result.size}")

    assert(result != null)
  }

  test("Get Dataset by name") {
    val name: String = "ZCACT"

    val dd = new DatasetDao
    val result = dd.getDatasetByName(name)
    logger.info(s"Dataset Size: ${result.id}")

    assert(result != null)
  }

  test("Get Datasets by Sequence of names") {
    val names: Seq[String] = Seq("MAGE_RANGE","MZACTREAS")

    val dd = new DatasetDao
    val result = dd.getDatasetsBySequenceNames(names)
    logger.info(s"Dataset size: ${result.size}")

    assert(result != null)
  }*/

}
