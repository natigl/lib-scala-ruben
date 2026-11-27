package com.gestamp.hypatia

import com.gestamp.hypatia.args.QualityParserArgs
import com.gestamp.hypatia.exceptions.QualityParserArgsException
import com.gestamp.hypatia.impl.Quality.startQualityIngestion
import com.gestamp.hypatia.postgresql.dao.{WorkflowTaskExecutionDao,WorkflowJobExecutionDao}
import com.gestamp.hypatia.postgresql.model.{WorkflowTaskExecution,WorkflowJobExecution}
import com.gestamp.hypatia.utils.WorkflowTags._
import com.gestamp.hypatia.utils.QualityTaskTags.{taskKey, taskRunId}
import com.gestamp.hypatia.spark.SparkSessionHolder
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

import com.gestamp.hypatia.spark.SparkConfiguration
import org.apache.spark.sql.SparkSession

object AppQuality {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val qualityTaskExecutionDao: WorkflowTaskExecutionDao = new WorkflowTaskExecutionDao
  private val jobExecutionDao: WorkflowJobExecutionDao = new WorkflowJobExecutionDao

  /**
   * Main del job de calidad en Databricks: crea una SparkSession propia, registra
   * la ejecución del job y de la task en orchestration, parsea los argumentos y
   * lanza el proceso de calidad, persistiendo el resultado (éxito/fallo y datasets OK).
   *
   * @param args argumentos de línea de comandos (-o source, -d datasets)
   */
  def main(args: Array[String]): Unit = {
    val qualityStartTime = System.currentTimeMillis()
    logger.info(s"Start Quality with args = ${args.mkString(",")}")

    val qualitySession = SparkSession.builder().getOrCreate().newSession()
    SparkSession.setActiveSession(qualitySession)
    SparkSession.setDefaultSession(qualitySession)
    SparkSessionHolder.set(qualitySession)

    // Force SparkConfig.json apply to new Quality spark session
    SparkConfiguration.spark

    // Checking whether the job run id has been already registered
    val qualityRegisteredJobRunId = jobExecutionDao.getWorkflowJobExecutionByJobRunId(jobRunID).exists(_.job_id == jobID)

    if (qualityRegisteredJobRunId) {
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

    QualityParserArgs.parserArgs(args) match {
      case Right(value) =>
        val qualityTaskExecutionInit = WorkflowTaskExecution(
          task_name = taskKey,
          task_run_id = taskRunId,
          job_run_id = jobRunID,
          task_process_utc_start_time = new Timestamp(qualityStartTime),
          parameters = args.mkString(", "),
          status = "Running"
        )
        qualityTaskExecutionDao.insertIntoOnConflict(qualityTaskExecutionInit)

        val qualityJob = Try(startQualityIngestion(value))
        val qualityEndTime = System.currentTimeMillis()
        val qualityDuration = (qualityEndTime - qualityStartTime) / 1000

        val qualityStatus = qualityJob match {
          case Success(successfulDatasets) =>
            qualityTaskExecutionInit.copy(
              task_process_duration = qualityDuration,
              task_process_utc_end_time = new Timestamp(qualityEndTime),
              status = "Success",
              successful_datasets = successfulDatasets.mkString(",")
            )

          case Failure(exception) =>
            qualityTaskExecutionInit.copy(
              task_process_duration = qualityDuration,
              task_process_utc_end_time = new Timestamp(qualityEndTime),
              status = "Failure",
              error_message = exception.getMessage.split("Stack trace:")(0).trim
            )
        }

        qualityTaskExecutionDao.insertIntoOnConflict(qualityStatus)

      case Left(exception) =>
        val qualityEndTime = System.currentTimeMillis()
        val qualityDuration = (qualityEndTime - qualityStartTime) / 1000
        val qualityTaskExecutionArgsFailure = WorkflowTaskExecution(
          task_name = taskKey,
          task_run_id = taskRunId,
          job_run_id = jobRunID,
          task_process_duration = qualityDuration,
          task_process_utc_start_time = new Timestamp(qualityStartTime),
          task_process_utc_end_time = new Timestamp(qualityEndTime),
          parameters = args.mkString(", "),
          status = "Failure",
          error_message = exception.getMessage
        )
        qualityTaskExecutionDao.insertIntoOnConflict(qualityTaskExecutionArgsFailure)
        throw new QualityParserArgsException(exception.getMessage, exception)
    }

    SparkSessionHolder.clear()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

  }
}
