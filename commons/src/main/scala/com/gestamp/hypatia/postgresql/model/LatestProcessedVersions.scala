package com.gestamp.hypatia.postgresql.model

import java.sql.Timestamp

case class LatestProcessedVersions(id: Long = 0,
                                   source: String,
                                   read_layer: String,
                                   read_table: String,
                                   last_read_processed_version: Long,
                                   target_layer: String,
                                   target_table: String,
                                   updated_at: Timestamp = null,
                                   task_run_id: String,
                                  )
