package com.gestamp.hypatia

import com.gestamp.hypatia.integration.args.IntegrationParserArgs
import com.gestamp.hypatia.integration.impl.Integration.startIntegration
import com.gestamp.hypatia.postgresql.dao.{WorkflowTaskExecutionDao,WorkflowJobExecutionDao}
import com.gestamp.hypatia.postgresql.model.{WorkflowTaskExecution,WorkflowJobExecution}

import com.gestamp.hypatia.utils.WorkflowTags._
import com.gestamp.hypatia.utils.IntegrationTaskTags.{taskKey, taskRunId}
import com.gestamp.hypatia.integration.exception.IntegrationParserArgsException
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession
import com.gestamp.hypatia.spark.SparkSessionHolder
import com.gestamp.hypatia.spark.SparkConfiguration

object AppIntegration {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val integrationTaskExecutionDao: WorkflowTaskExecutionDao = new WorkflowTaskExecutionDao
  private val jobExecutionDao: WorkflowJobExecutionDao = new WorkflowJobExecutionDao

  /**
   * Main del job de integración en Databricks: registra la ejecución del job y de la
   * task en orchestration, parsea los argumentos y lanza la integración, persistiendo
   * el resultado (éxito/fallo y distillations OK).
   *
   * @param args argumentos de línea de comandos (-o source, -d datasets)
   */
  def main(args: Array[String]): Unit = {
    val integrationStartTime = System.currentTimeMillis()
    logger.info(s"Start Integration with args = ${args.mkString(",")}")

    val integrationSession = SparkSession.builder().getOrCreate().newSession()
    SparkSession.setActiveSession(integrationSession)
    SparkSession.setDefaultSession(integrationSession)
    SparkSessionHolder.set(integrationSession)

    // Force SparkConfig.json apply to new Integration spark session
    SparkConfiguration.spark

    // Checking whether the job run id has been already registered
    val integrationRegisteredJobRunId = jobExecutionDao.getWorkflowJobExecutionByJobRunId(jobRunID).exists(_.job_id == jobID)

    if (integrationRegisteredJobRunId) {
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

    IntegrationParserArgs.parserArgs(args) match {
      case Right(value) =>
        val integrationTaskExecutionInit = WorkflowTaskExecution(
          task_name = taskKey,
          task_run_id = taskRunId,
          job_run_id = jobRunID,
          task_process_utc_start_time = new Timestamp(integrationStartTime),
          parameters = args.mkString(", "),
          status = "Running"
        )
        integrationTaskExecutionDao.insertIntoOnConflict(integrationTaskExecutionInit)

        val integrationJob = Try(startIntegration(value))
        val integrationEndTime = System.currentTimeMillis()
        val integrationDuration = (integrationEndTime - integrationStartTime) / 1000

        val integrationStatus = integrationJob match {
          case Success(successfulDatasets) =>
            integrationTaskExecutionInit.copy(
              task_process_duration = integrationDuration,
              task_process_utc_end_time = new Timestamp(integrationEndTime),
              status = "Success",
              successful_datasets = successfulDatasets.mkString(",")
            )

          case Failure(exception) =>
            integrationTaskExecutionInit.copy(
              task_process_duration = integrationDuration,
              task_process_utc_end_time = new Timestamp(integrationEndTime),
              status = "Failure",
              error_message = exception.getMessage.split("Stack trace:")(0).trim
            )
        }
        integrationTaskExecutionDao.insertIntoOnConflict(integrationStatus)

      case Left(exception) =>
        val integrationEndTime = System.currentTimeMillis()
        val integrationDuration = (integrationEndTime - integrationStartTime) / 1000
        val integrationTaskExecutionArgsFailure = WorkflowTaskExecution(
          task_name = taskKey,
          task_run_id = taskRunId,
          job_run_id = jobRunID,
          task_process_duration = integrationDuration,
          task_process_utc_start_time = new Timestamp(integrationStartTime),
          task_process_utc_end_time = new Timestamp(integrationEndTime),
          parameters = args.mkString(", "),
          status = "Failure",
          error_message = exception.getMessage
        )
        integrationTaskExecutionDao.insertIntoOnConflict(integrationTaskExecutionArgsFailure)
        throw new IntegrationParserArgsException(exception.getMessage, exception)
    }
  }

}
