package com.gestamp.hypatia.utils

object IngestionConstants {

  val STAGING_LAYER = "staging"
  val BRONZE_LAYER = "bronze"

  val SOURCE_PARAM = "source"
  val PATH = "path"
  val FORMAT_FILE = "format_file"
  val USE_CASE_PARAM = "useCase"
  val DOMAIN_PARAM = "domain"
  val SUBDOMAIN_PARAM = "subdomain"
  val DATASET_PARAM = "dataset"

  val OUTPUT_MODE_APPEND = "append"
  val OUTPUT_MODE_COMPLETE = "complete"
  val OUTPUT_MODE_UPDATE = "update"
  val OUTPUT_MODE_OVERWRITE = "overwrite"

  val FULL_EXTRACTION = "FULL"
  val DELTA_EXTRACTION = "DELTA"
  val CDC_EXTRACTION = "CDC"
  val FULL_PARTITION_EXTRACTION = "FULL_PARTITION"
  val STREAM_EXTRACTION = "STREAM"

  val SAP_ECC_SOURCE = "SAP_ECC"
  val SAP_SNP_SOURCE = "SAP_SNP"

}
