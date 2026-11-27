package com.gestamp.hypatia.args

import com.gestamp.hypatia.exceptions.IngestionParserArgsException
import com.gestamp.hypatia.postgresql.model.Dataset
import org.scalatest.{FunSuite, Matchers}

class IngestionParserArgsTest extends FunSuite with Matchers {

  val inputDatasetsId = "[3,6]"
  val args = Array("-o", "BW_Human_Resources", "-d", inputDatasetsId)

  val inputDatasetsId_FAIL = "[3,6]"
  val args_FAIL = Array("-o", "BW_Human_Resources", "-g", inputDatasetsId)

  val expectedDatasets: Seq[Dataset] = Seq(
    Dataset(id = 3, dataset = "MAGE_RANGE", extract_type = "FULL",
      delta_column = "", format = "csv", blocksizeinmb = 100, mappings = "", header = true, trigger_id = 2,
      active = true,  group = "Maestros", delimiter = ",", key_columns = ""),
    Dataset(id = 6, dataset = "MZACTREAS", extract_type = "FULL",
      delta_column = "", format = "csv", blocksizeinmb = 100, mappings = "", header = true, trigger_id = 2,
      active = true,  group = "Maestros", delimiter = ",", key_columns = ""))

  val ingestionParamsExpected: IngestionJobParams = IngestionJobParams(source = "BW_Human_Resources",
    datasets = expectedDatasets)


 /* test("Testing Ingestion parser input parameters OK") {
    val result: Either[Throwable, IngestionJobParams] = IngestionParserArgs.parserArgs(args)
    assert(result.right.get == ingestionParamsExpected)
  }


  test("Testing Ingestion parser input parameters FAIL") {
    assertThrows[IngestionParserArgsException](
      IngestionParserArgs.parserArgs(args_FAIL)
    )
  }*/

}
