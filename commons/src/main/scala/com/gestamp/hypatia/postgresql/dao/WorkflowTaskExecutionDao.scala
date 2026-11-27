package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iWorkflowTaskExecutionDao
import com.gestamp.hypatia.postgresql.model.WorkflowTaskExecution
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{PreparedStatement, ResultSet}

/**
 * DAO de la tabla orchestration.workflow_task_execution: registro de la ejecución
 * de cada task de un job de Databricks (duración, estado, datasets OK, error...).
 */
class WorkflowTaskExecutionDao() extends PostgreSqlManager[WorkflowTaskExecution](schema = "orchestration") with iWorkflowTaskExecutionDao{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val table = "workflow_task_execution"

  /**
   * Devuelve las ejecuciones de tasks asociadas a un job_run_id.
   *
   * @param job_run_id id de la ejecución del job
   * @return las ejecuciones de tasks encontradas
   */
  def getWorkflowTaskExecutionByJobRunId(job_run_id: String): Seq[WorkflowTaskExecution] = {
    val whereSql =
      s"""job_run_id = '$job_run_id'""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Ejecuta la consulta y mapea las filas a WorkflowTaskExecution.
   *
   * @param sql sentencia a ejecutar
   * @return las ejecuciones resultantes
   */
  private def select(sql: String): Seq[WorkflowTaskExecution] = {
    this.runSelect(sql)
  }

  /**
   * Construye la SELECT completa sobre workflow_task_execution con la condición WHERE dada.
   *
   * @param whereSql condición WHERE de la consulta
   * @return la sentencia SQL completa
   */
  protected def createSql(whereSql: String): String = {
    s"""SELECT *
       | FROM ${schema + "." + table}
       | WHERE $whereSql""".stripMargin
  }

  /**
   * Inserta la ejecución de la task; si ya existe (mismo task_run_id) la actualiza (upsert).
   *
   * @param insertModel ejecución de task a insertar o actualizar
   */
  def insertIntoOnConflict(insertModel: WorkflowTaskExecution): Unit = {
    val sql =
      s"""INSERT INTO ${schema+"."+table}
         |(task_name, task_run_id, job_run_id, task_process_duration, task_process_utc_start_time, task_process_utc_end_time, parameters, status, successful_datasets, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         |ON CONFLICT (task_run_id)
         |DO
         |   UPDATE SET
         |  task_name= EXCLUDED.task_name,
         |	job_run_id= EXCLUDED.job_run_id,
         |	task_process_duration=EXCLUDED.task_process_duration,
         |	task_process_utc_start_time=EXCLUDED.task_process_utc_start_time,
         |	task_process_utc_end_time=EXCLUDED.task_process_utc_end_time,
         |	parameters=EXCLUDED.parameters,
         |	status=EXCLUDED.status,
         | 	successful_datasets=EXCLUDED.successful_datasets,
         |	error_message=EXCLUDED.error_message;""".stripMargin
    this.runInsert(sql, insertModel)
  }

  /**
   * Mapea una fila del ResultSet al modelo WorkflowTaskExecution.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la WorkflowTaskExecution construida a partir de la fila
   */
  def mapperRowdata(resultSet: ResultSet): WorkflowTaskExecution = {
    logger.info(s"Mapping Task Execution")

    WorkflowTaskExecution(
      id = resultSet.getInt("id"),
      task_name = resultSet.getString("task_name"),
      task_run_id = resultSet.getString("task_run_id"),
      job_run_id = resultSet.getString("job_run_id"),
      task_process_duration = resultSet.getLong("task_process_duration"),
      task_process_utc_start_time = resultSet.getTimestamp("task_process_utc_start_time"),
      task_process_utc_end_time = resultSet.getTimestamp("task_process_utc_end_time"),
      parameters = resultSet.getString("parameters"),
      status = resultSet.getString("status"),
      successful_datasets = resultSet.getString("successful_datasets"),
      error_message = resultSet.getString("error_message")
    )
  }

  /**
   * Rellena los parámetros del INSERT/UPSERT a partir del modelo.
   *
   * @param preparedStatement statement de inserción a completar
   * @param insertModel       ejecución de task con los valores a insertar
   * @return el statement con los parámetros ya asignados
   */
  override def setInsert(preparedStatement: PreparedStatement, insertModel: WorkflowTaskExecution): PreparedStatement = {
    logger.info(s"Setting values for Autoloader job execution prepared statement")

    preparedStatement.setString(1, insertModel.task_name)
    preparedStatement.setString(2, insertModel.task_run_id)
    preparedStatement.setString(3,insertModel.job_run_id)
    preparedStatement.setLong(4,insertModel.task_process_duration)
    preparedStatement.setTimestamp(5,insertModel.task_process_utc_start_time)
    preparedStatement.setTimestamp(6,insertModel.task_process_utc_end_time)
    preparedStatement.setString(7,insertModel.parameters)
    preparedStatement.setString(8, insertModel.status)
    preparedStatement.setString(9, insertModel.successful_datasets)
    preparedStatement.setString(10, insertModel.error_message)

    preparedStatement
  }
}
