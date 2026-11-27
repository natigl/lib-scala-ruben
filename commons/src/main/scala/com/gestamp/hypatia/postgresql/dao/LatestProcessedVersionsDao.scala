package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iLatestProcessedVersions
import com.gestamp.hypatia.postgresql.model.LatestProcessedVersions
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{PreparedStatement, ResultSet}

/**
 * DAO de la tabla orchestration.latest_processed_versions: última versión Delta
 * procesada por cada flujo (source, tabla leída, capa leída → tabla/capa destino),
 * usada para lecturas incrementales entre capas.
 */
class LatestProcessedVersionsDao() extends PostgreSqlManager[LatestProcessedVersions](schema = "orchestration") with iLatestProcessedVersions{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val table = "latest_processed_versions"

  /**
   * Devuelve el registro de última versión procesada para la clave única del flujo
   * (source, capa/tabla de lectura, capa/tabla destino).
   *
   * @param source       source del dato
   * @param read_layer   capa de la que se lee (p.ej. bronze)
   * @param read_table   tabla de la que se lee
   * @param target_layer capa destino (p.ej. silver)
   * @param target_table tabla destino
   * @return el registro encontrado (vacío si el flujo nunca se procesó)
   */
  def getLatestProcessedVersionsByUniqueKey(source: String, read_layer: String, read_table:String, target_layer: String, target_table:String): Seq[LatestProcessedVersions] = {
    val whereSql =
      s"""source = '$source' AND read_layer = '$read_layer' AND read_table = '$read_table' AND target_layer = '$target_layer' AND target_table = '$target_table'""".stripMargin
    select(createSql(whereSql))
  }

  /**
   * Ejecuta la consulta y mapea las filas a LatestProcessedVersions.
   *
   * @param sql sentencia a ejecutar
   * @return los registros resultantes
   */
  private def select(sql: String): Seq[LatestProcessedVersions] = {
    this.runSelect(sql)
  }

  /**
   * Construye la SELECT completa sobre latest_processed_versions con la condición WHERE dada.
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
   * Inserta el registro de última versión procesada; si ya existe la clave única
   * del flujo, actualiza la versión, la fecha y el task_run_id (upsert).
   *
   * @param insertModel registro a insertar o actualizar
   */
  def insertIntoOnConflict(insertModel: LatestProcessedVersions): Unit = {
    val sql =
      s"""INSERT INTO ${schema+"."+table}
         |(source, read_layer, read_table, last_read_processed_version, target_layer, target_table, task_run_id) VALUES (?, ?, ?, ?, ?, ?, ?)
         |ON CONFLICT (source, read_layer, read_table, target_layer, target_table)
         |DO UPDATE SET
         |	last_read_processed_version=EXCLUDED.last_read_processed_version,
         |	updated_at=now(),
         | 	task_run_id=EXCLUDED.task_run_id;""".stripMargin
    this.runInsert(sql, insertModel)
  }

  /**
   * Mapea una fila del ResultSet al modelo LatestProcessedVersions.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el LatestProcessedVersions construido a partir de la fila
   */
  def mapperRowdata(resultSet: ResultSet): LatestProcessedVersions = {
    logger.info(s"Mapping LatestProcessedVersions Row")

    LatestProcessedVersions(
      id = resultSet.getLong("id"),
      source = resultSet.getString("source"),
      read_layer = resultSet.getString("read_layer"),
      read_table = resultSet.getString("read_table"),
      last_read_processed_version = resultSet.getLong("last_read_processed_version"),
      target_layer = resultSet.getString("target_layer"),
      target_table = resultSet.getString("target_table"),
      updated_at = resultSet.getTimestamp("updated_at"),
      task_run_id = resultSet.getString("task_run_id")
    )
  }

  /**
   * Rellena los parámetros del INSERT/UPSERT a partir del modelo.
   *
   * @param preparedStatement statement de inserción a completar
   * @param insertModel       registro con los valores a insertar
   * @return el statement con los parámetros ya asignados
   */
  override def setInsert(preparedStatement: PreparedStatement, insertModel: LatestProcessedVersions): PreparedStatement = {
    logger.info(s"Setting values for Autoloader job execution prepared statement")

    preparedStatement.setString(1, insertModel.source)
    preparedStatement.setString(2, insertModel.read_layer)
    preparedStatement.setString(3,insertModel.read_table)
    preparedStatement.setLong(4,insertModel.last_read_processed_version)
    preparedStatement.setString(5,insertModel.target_layer)
    preparedStatement.setString(6,insertModel.target_table)
    preparedStatement.setString(7, insertModel.task_run_id)

    preparedStatement
  }
}
