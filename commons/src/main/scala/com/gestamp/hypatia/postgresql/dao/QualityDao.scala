package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iQualityDao
import com.gestamp.hypatia.postgresql.model.{Dataset, Quality}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.quality: reglas de calidad definidas por dataset
 * (regla, valor, campos afectados y reemplazo).
 */
class QualityDao extends PostgreSqlManager[Quality] with iQualityDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "quality"

  /**
   * Devuelve la primera regla de calidad activa de un dataset.
   *
   * @param dataset_id id del dataset
   * @return la primera regla activa encontrada
   */
  def getRuleById(dataset_id: Int): Quality = {
    getRulesById(dataset_id).head
  }

  /**
   * Devuelve las reglas de calidad activas de un dataset, ordenadas por id.
   *
   * @param dataset_id id del dataset
   * @return las reglas activas encontradas
   */
  def getRulesById(dataset_id: Int): Seq[Quality] = {
    val whereSql =
      s"""dataset_id = $dataset_id and active = true order by id""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve las reglas de calidad de los datasets cuyos ids estén en la lista,
   * ordenadas por id (incluye también las inactivas).
   *
   * @param dataset_id ids de dataset
   * @return las reglas encontradas
   */
  def getRulesBySequenceIds(dataset_id: Seq[Int]): Seq[Quality] = {
    val whereSql =
      s"""dataset_id in (${dataset_id.mkString(",")}) order by id""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Construye la SELECT completa sobre la tabla quality con la condición WHERE dada.
   *
   * @param whereSql condición WHERE de la consulta
   * @return la sentencia SQL completa
   */
  protected def createSql(whereSql: String): String = {
    s"""SELECT *
       | FROM ${schema+"."+table}
       | WHERE $whereSql""".stripMargin
  }

  /**
   * Ejecuta la consulta y mapea las filas a Quality.
   *
   * @param sql sentencia a ejecutar
   * @return las reglas resultantes
   */
  private def select(sql: String): Seq[Quality] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo Quality.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la Quality construida a partir de la fila
   */
  override def mapperRowdata(resultSet: ResultSet): Quality = {
    logger.info(s"Mapping Dataset Row: ${resultSet.getString("id")}")

    Quality(
      id = resultSet.getInt("id"),
      name = resultSet.getString("name"),
      dataset_id = resultSet.getInt("dataset_id"),
      rule = resultSet.getString("rule"),
      value = resultSet.getString("value"),
      fields = resultSet.getString("fields"),
      description = resultSet.getString("description"),
      active = resultSet.getBoolean("active"),
      replacement = resultSet.getString("replacement")
    )
  }
}
