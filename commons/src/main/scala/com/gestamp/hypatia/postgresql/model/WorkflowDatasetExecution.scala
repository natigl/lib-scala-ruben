package com.gestamp.hypatia.postgresql.model

import java.sql.Timestamp

case class WorkflowDatasetExecution(id: Int = 0,
                                    dataset: String,
                                    source: String,
                                    task_name: String,
                                    task_run_id: String, // FK
                                    dataset_process_duration: Long = 0,
                                    dataset_process_utc_start_time: Timestamp = null,
                                    dataset_process_utc_end_time: Timestamp = null,
                                    status: String = "Not processed",
                                    error_message: String = "",
                                    read_rows: Long = 0,
                                    written_rows: Long = 0
                                    )
