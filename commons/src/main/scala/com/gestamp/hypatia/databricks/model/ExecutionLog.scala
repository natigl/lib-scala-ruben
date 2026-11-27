package com.gestamp.hypatia.databricks.model

import java.sql.Timestamp

case class ExecutionLog(jobName: String,
                        taskKey: String,
                        jobID: String,
                        jobRunID: String,
                        runID: String,
                        databricksExecutionID: String,
                        started: Timestamp,
                        datasets: Seq[DatasetLog] = List.empty)

case class DatasetLog(dataset: String, status: Boolean = true, message: String = "", rowsAffected: Int = 0)
