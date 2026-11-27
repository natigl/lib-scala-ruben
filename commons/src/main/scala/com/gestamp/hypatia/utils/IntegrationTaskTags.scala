package com.gestamp.hypatia.utils

import com.databricks.dbutils_v1.{DBUtilsHolder, DBUtilsV1}

object IntegrationTaskTags {

  private val dbutils: DBUtilsV1 = DBUtilsHolder.dbutils

  // Task values
  val taskKey = dbutils.notebook.getContext().tags("taskKey") // Job task name
  val taskRunId = dbutils.notebook.getContext().tags("runId") // Job task run id
}