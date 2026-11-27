package com.gestamp.hypatia.config

import com.gestamp.hypatia.postgresql.model.Dataset
import org.scalatest.FunSuite

class AutoLoaderHelperTest extends FunSuite {

  val datasetCSV = new Dataset(
    id = 1,
    dataset ="name",
    extract_type = "FULL",
    delta_column = "",
    format = "CSV",
    blocksizeinmb = 100,
    mappings = "",
    header = true,
    trigger_id = 1,
    active = true,
    group = "test",
    delimiter = ",",
    key_columns = "")

  val source = "source" // Define el valor del origen

 /* test("getAutoLoaderConfig should return the correct configuration for CSV datasets") {

    val expected = Map(
      CloudFiles.FORMAT -> "csv",
      CloudFiles.SCHEMA_EVOLUTION_MODE -> "failOnNewColumns",
      CloudFiles.SCHEMA_LOCATION -> "",
      CloudFiles.DELIMITER -> ",",
      CloudFiles.HEADER -> "true",
      CloudFiles.MULTILINE -> "true"
    )
    val result = AutoLoaderConfigBuilder(datasetCSV, source).getAutoLoaderConfig()
    assert(result == expected)
  }*/

}
