package com.gestamp.hypatia.postgresql.model

import java.sql.Timestamp

case class WorkflowJobExecution(id: Int = 0,
                                job_name: String,
                                job_id: String,
                                job_run_id: String,
                                job_utc_trigger_time: Timestamp
                                )
