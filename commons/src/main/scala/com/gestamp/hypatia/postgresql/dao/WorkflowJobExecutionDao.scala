package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iWorkflowJobExecutionDao
import com.gestamp.hypatia.postgresql.model.WorkflowJobExecution
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{PreparedStatement, ResultSet}

/**
 * DAO de la tabla orchestration.workflow_job_execution: registro de cada
 * ejecución de job de Databricks (nombre, ids y hora de disparo).
 */
class WorkflowJobExecutionDao() extends PostgreSqlManager[WorkflowJobExecution](schema = "orchestration") with iWorkflowJobExecutionDao{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val table = "workflow_job_execution"

  /**
   * Devuelve las ejecuciones de job con el job_run_id indicado.
   *
   * @param job_run_id id de la ejecución del job
   * @return las ejecuciones encontradas
   */
  def getWorkflowJobExecutionByJobRunId(job_run_id: String): Seq[WorkflowJobExecution] = {
    val whereSql =
      s"""job_run_id = '$job_run_id'""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Ejecuta la consulta y mapea las filas a WorkflowJobExecution.
   *
   * @param sql sentencia a ejecutar
   * @return las ejecuciones resultantes
   */
  private def select(sql: String): Seq[WorkflowJobExecution] = {
    this.runSelect(sql)
  }

  /**
   * Construye la SELECT completa sobre workflow_job_execution con la condición WHERE dada.
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
   * Inserta la ejecución del job; si ya existe (mismo job_run_id) la actualiza (upsert).
   *
   * @param insertModel ejecución de job a insertar o actualizar
   */
  def insertIntoOnConflict(insertModel: WorkflowJobExecution): Unit = {
    val sql =
      s"""INSERT INTO ${schema+"."+table}
         |(job_name, job_id, job_run_id, job_utc_trigger_time) VALUES (?, ?, ?, ?)
         |ON CONFLICT (job_run_id)
         |DO
         |   UPDATE SET
         |  job_name= EXCLUDED.job_name,
         |	job_id= EXCLUDED.job_id,
         |	job_utc_trigger_time=EXCLUDED.job_utc_trigger_time;""".stripMargin
    this.runInsert(sql, insertModel)
  }

  /**
   * Mapea una fila del ResultSet al modelo WorkflowJobExecution.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la WorkflowJobExecution construida a partir de la fila
   */
  def mapperRowdata(resultSet: ResultSet): WorkflowJobExecution = {
    logger.info(s"Mapping Job Execution")

    WorkflowJobExecution(
      id = resultSet.getInt("id"),
      job_name = resultSet.getString("job_name"),
      job_id = resultSet.getString("job_id"),
      job_run_id = resultSet.getString("job_run_id"),
      job_utc_trigger_time = resultSet.getTimestamp("job_utc_trigger_time")
    )
  }

  /**
   * Rellena los parámetros del INSERT/UPSERT a partir del modelo.
   *
   * @param preparedStatement statement de inserción a completar
   * @param insertModel       ejecución de job con los valores a insertar
   * @return el statement con los parámetros ya asignados
   */
  override def setInsert(preparedStatement: PreparedStatement, insertModel: WorkflowJobExecution): PreparedStatement = {
    logger.info(s"Setting values for Autoloader job execution prepared statement")

    preparedStatement.setString(1, insertModel.job_name)
    preparedStatement.setString(2, insertModel.job_id)
    preparedStatement.setString(3,insertModel.job_run_id)
    preparedStatement.setTimestamp(4,insertModel.job_utc_trigger_time)

    preparedStatement
  }
}
