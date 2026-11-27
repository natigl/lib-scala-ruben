package com.gestamp.hypatia.utils.monitoring

import java.sql.Timestamp

case class TableMonitoringResult(
                                  table_name: String,
                                  raw_sources_lineage: String,
                                  raw_clean_lineage: String,
                                  integration_lineage: String,
                                  semantic_lineage: String,
                                  last_load_gold: Timestamp,
                                  updated_after_gold: String,
                                  updated_before_gold : String,
                                  gold_status : String,
                                  row_count_diff: String,
                                  schema_diff: String,
                                  monitoring_date: Timestamp
                                )

