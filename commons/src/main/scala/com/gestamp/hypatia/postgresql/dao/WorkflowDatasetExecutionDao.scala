package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iWorkflowDatasetExecutionDao
import com.gestamp.hypatia.postgresql.model.WorkflowDatasetExecution
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{PreparedStatement, ResultSet}

/**
 * DAO de la tabla orchestration.workflow_dataset_execution: registro del
 * procesamiento de cada dataset dentro de una task (duración, estado, filas
 * leídas/escritas y error si lo hubo).
 */
class WorkflowDatasetExecutionDao() extends PostgreSqlManager[WorkflowDatasetExecution](schema = "orchestration") with iWorkflowDatasetExecutionDao{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val table = "workflow_dataset_execution"

  /**
   * Inserta el registro de ejecución del dataset.
   *
   * @param insertModel ejecución de dataset a insertar
   */
  def insertInto(insertModel: WorkflowDatasetExecution): Unit = {
    val sql =
      s"""INSERT INTO ${schema+"."+table}
         |(dataset, source, task_name, task_run_id, dataset_process_duration, dataset_process_utc_start_time, dataset_process_utc_end_time, status, error_message, read_rows, written_rows) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
    this.runInsert(sql, insertModel)
  }

  /**
   * Mapea una fila del ResultSet al modelo WorkflowDatasetExecution.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la WorkflowDatasetExecution construida a partir de la fila
   */
  def mapperRowdata(resultSet: ResultSet): WorkflowDatasetExecution = {
    logger.info(s"Mapping Dataset Execution")

    WorkflowDatasetExecution(
      id = resultSet.getInt("id"),
      dataset = resultSet.getString("dataset"),
      source = resultSet.getString("source"),
      task_name = resultSet.getString("task_name"),
      task_run_id = resultSet.getString("task_run_id"),
      dataset_process_duration = resultSet.getInt("dataset_process_duration"),
      dataset_process_utc_start_time = resultSet.getTimestamp("dataset_process_utc_start_time"),
      dataset_process_utc_end_time = resultSet.getTimestamp("dataset_process_utc_end_time"),
      status = resultSet.getString("status"),
      error_message = resultSet.getString("message_error"),
      read_rows = resultSet.getLong("read_rows"),
      written_rows = resultSet.getLong("written_rows")
    )
  }

  /**
   * Rellena los parámetros del INSERT a partir del modelo.
   *
   * @param preparedStatement statement de inserción a completar
   * @param insertModel       ejecución de dataset con los valores a insertar
   * @return el statement con los parámetros ya asignados
   */
  override def setInsert(preparedStatement: PreparedStatement, insertModel: WorkflowDatasetExecution): PreparedStatement = {
    logger.info(s"Setting values for Dataset execution prepared statement")

    preparedStatement.setString(1, insertModel.dataset)
    preparedStatement.setString(2, insertModel.source)
    preparedStatement.setString(3,insertModel.task_name)
    preparedStatement.setString(4,insertModel.task_run_id)
    preparedStatement.setLong(5,insertModel.dataset_process_duration)
    preparedStatement.setTimestamp(6,insertModel.dataset_process_utc_start_time)
    preparedStatement.setTimestamp(7,insertModel.dataset_process_utc_end_time)
    preparedStatement.setString(8, insertModel.status)
    preparedStatement.setString(9, insertModel.error_message)
    preparedStatement.setLong(10, insertModel.read_rows)
    preparedStatement.setLong(11, insertModel.written_rows)

    preparedStatement
  }
}
