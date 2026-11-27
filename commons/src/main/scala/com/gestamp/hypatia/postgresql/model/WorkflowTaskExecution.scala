package com.gestamp.hypatia.postgresql.model

import java.sql.Timestamp

case class WorkflowTaskExecution(id: Int = 0,
                                 task_name: String,
                                 task_run_id: String,
                                 job_run_id: String,
                                 task_process_duration: Long = 0,
                                 task_process_utc_start_time: Timestamp = null,
                                 task_process_utc_end_time: Timestamp = null,
                                 parameters: String = "",
                                 status: String = "Not processed",
                                 successful_datasets: String = "",
                                 error_message: String = ""
                                  )
