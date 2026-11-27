package com.gestamp.hypatia

import com.gestamp.hypatia.args.IngestionParserArgs
import com.gestamp.hypatia.exceptions.IngestionParserArgsException
import com.gestamp.hypatia.impl.IngestionLogic.startIngestion
import com.gestamp.hypatia.postgresql.dao.{WorkflowTaskExecutionDao,WorkflowJobExecutionDao}
import com.gestamp.hypatia.postgresql.model.{WorkflowTaskExecution,WorkflowJobExecution}
import com.gestamp.hypatia.utils.WorkflowTags._
import com.gestamp.hypatia.utils.IngestionTaskTags.{taskKey, taskRunId}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}


object Ingestion {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val autoloaderTaskExecutionDao: WorkflowTaskExecutionDao = new WorkflowTaskExecutionDao
  private val jobExecutionDao: WorkflowJobExecutionDao = new WorkflowJobExecutionDao

  /**
   * Método main de entrada a Databricks Auto Loader
   * @param args Input System Args
   */
  def main(args: Array[String]): Unit = {
    val autoloaderStartTime = System.currentTimeMillis()
    logger.info(s"Start of Ingestion with args = ${args.mkString(",")}")

    // Checking whether the job run id has been already registered
    val autoloaderRegisteredJobRunId = jobExecutionDao.getWorkflowJobExecutionByJobRunId(jobRunID).exists(_.job_id == jobID)

    if(autoloaderRegisteredJobRunId) {
      logger.info(s"The job run id $jobRunID has been already registered by a previous task")
    } else {
      jobExecutionDao.insertIntoOnConflict(
        WorkflowJobExecution(
          job_name = jobName,
          job_id = jobID,
          job_run_id = jobRunID,
          job_utc_trigger_time = new Timestamp(jobTriggerTime)
        )
      )
      logger.info(s"The job run id $jobRunID has been registered in PostgreSQL")
    }

    IngestionParserArgs.parserArgs(args) match {
      case Right(value) =>
        val autoloaderTaskExecutionInit = WorkflowTaskExecution(
          task_name = taskKey,
          task_run_id = taskRunId,
          job_run_id = jobRunID,
          task_process_utc_start_time = new Timestamp(autoloaderStartTime),
          parameters = args.mkString(", "),
          status = "Running"
        )

        autoloaderTaskExecutionDao.insertIntoOnConflict(autoloaderTaskExecutionInit)

        val autoloaderJob = Try(startIngestion(value))
        val autoloaderEndTime = System.currentTimeMillis()
        val autoloaderDuration = (autoloaderEndTime - autoloaderStartTime) / 1000

        val autoloaderStatus = autoloaderJob match {
          case Success(successfulDatasets) =>
            autoloaderTaskExecutionInit.copy(
              task_process_duration = autoloaderDuration,
              task_process_utc_end_time = new Timestamp(autoloaderEndTime),
              status = "Success",
              successful_datasets = successfulDatasets.mkString(",")
            )
          case Failure(exception) =>
            autoloaderTaskExecutionInit.copy(
              task_process_duration = autoloaderDuration,
              task_process_utc_end_time = new Timestamp(autoloaderEndTime),
              status = "Failure",
              error_message = exception.getMessage.split("Stack trace:")(0).trim
            )
        }
        autoloaderTaskExecutionDao.insertIntoOnConflict(autoloaderStatus)

      case Left(exception) =>
        val autoloaderEndTime = System.currentTimeMillis()
        val autoloaderDuration = (autoloaderEndTime - autoloaderStartTime) / 1000
        val autoloaderTaskExecutionArgsFailure = WorkflowTaskExecution(
          task_name = taskKey,
          task_run_id = taskRunId,
          job_run_id = jobRunID,
          task_process_duration = autoloaderDuration,
          task_process_utc_start_time = new Timestamp(autoloaderStartTime),
          task_process_utc_end_time = new Timestamp(autoloaderEndTime),
          parameters = args.mkString(", "),
          status = "Failure",
          error_message = exception.getMessage
        )
        autoloaderTaskExecutionDao.insertIntoOnConflict(autoloaderTaskExecutionArgsFailure)
        throw new IngestionParserArgsException(exception.getMessage, exception)
    }
  }
}
