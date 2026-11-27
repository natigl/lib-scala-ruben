package com.gestamp.hypatia.utils

import com.gestamp.hypatia.databricks.DatabricksManager

object CommonsConstants {

  val DATE_FORMAT = "dd/MM/yyyy"

  val DELTA_FORMAT = "delta"
  val PARQUET_FORAMT = "parquet"
  val CSV_FORAMT = "csv"
  val JSON_FORMAT = "json"
  val AVRO_FORMAT = "avro"
  val ORC_FORMAT = "orc"
  val TEXT_FROMAT = "text"
  val CDC = "CDC"
  val DELTA = "DELTA"
  val SAP_ECC = "SAP_ECC"
  val SAP_SNP = "SAP_SNP"
  val SAP_ARIBA = "SAP_Ariba"
  val STREAM_EXTRACTION = "STREAM"

  val FILE_NAME_CONFIG_SPARK = "SparkConfig.json"

  val DB_ADF_SCOPE_NAME_LAB = "hypatia-secrets-scope"

  val databricksManager = new DatabricksManager()
  val RAW_SOURCES = databricksManager.dbUtilsGetSecret("env") match {
    case x if x.equals("pro") => "raw_sources"
    case x if x.equals("lab") || x.equals("dev") || x.equals("pre") => s"raw_sources_${x}"
  }

  val RAW_CLEAN = databricksManager.dbUtilsGetSecret("env") match {
    case x if x.equals("pro") => "raw_clean"
    case x if x.equals("lab") || x.equals("dev")  || x.equals("pre") => s"raw_clean_${x}"
  }

  val INTEGRATION = databricksManager.dbUtilsGetSecret("env") match {
    case x if x.equals("pro") => "integration"
    case x if x.equals("lab") || x.equals("dev")  || x.equals("pre") => s"integration_${x}"
  }

  val SEMANTIC = databricksManager.dbUtilsGetSecret("env") match {
    case x if x.equals("pro") => "semantic"
    case x if x.equals("lab") || x.equals("dev")  || x.equals("pre") => s"semantic_${x}"
  }

  val ENV = databricksManager.dbUtilsGetSecret("env")

  val ENV_SOURCE = databricksManager.dbUtilsGetSecret("env") match {
    case x if x.equals("pro") => ""
    case x if x.equals("lab") || x.equals("dev")  || x.equals("pre") => s"_$x"
  }

  val SNP = "SNP"
}
