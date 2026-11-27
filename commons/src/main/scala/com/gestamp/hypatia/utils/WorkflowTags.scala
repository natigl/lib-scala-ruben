package com.gestamp.hypatia.utils

import com.databricks.dbutils_v1.{DBUtilsHolder, DBUtilsV1}

object WorkflowTags {

  private val dbutils: DBUtilsV1 = DBUtilsHolder.dbutils

  // Job values
  val jobID = dbutils.notebook.getContext().tags("jobId") // Workflow job id
  val jobRunID = dbutils.notebook.getContext().tags("jobRunId") // Workflow job run id
  val jobName = dbutils.notebook.getContext().tags("jobName") // Workflow job name
  val jobTriggerTime = dbutils.notebook.getContext().tags.get("jobTriggerTime").map(_.toLong).getOrElse(System.currentTimeMillis()) // Workflow job trigger time
  val databricksExecutionID = dbutils.notebook.getContext().tags("executionId") // job id + run id + action id

}