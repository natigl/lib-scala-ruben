package com.gestamp.hypatia.utils

object ConstantsLakehouse {

  val TABLE_PARAM = "tables"
  val TARGET_LAYER_PARAM = "target_layer"
  val ACTION_PARAM = "action"
  val SOURCE_PARAM = "source"
  val SCHEMA_EVOLUTION_ID_PARAM = "schema_evolution_id"
  val DELTA_VERSION_PARAM = "delta_version"


  val TOTAL_HISTORICAL_TABLE_RELOAD = "total_historical_reload"
  val DELTA_VERSION_TABLE_RELOAD = "delta_version_reload"
  val UPDATE_SCHEMA_BY_CSV = "update_schema_integration"
  val DELETE_COLUMN = "delete_column"
  val ADD_COLUMN = "add_column"
  val RENAME_COLUMN = "rename_column"
  val COLUMN_TYPING = "column_typing"

  val RAW_SOURCES_LAYER = "raw_sources"
  val RAW_CLEAN_LAYER = "raw_clean"
  val INTEGRATION_LAYER = "integration"
}
