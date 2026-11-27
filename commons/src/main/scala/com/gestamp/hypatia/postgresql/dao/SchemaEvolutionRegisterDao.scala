package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iSchemaEvolutionRegisterDao
import com.gestamp.hypatia.postgresql.model.SchemaEvolutionRegister
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.schema_evolution_register: acciones de evolución de
 * esquema (añadir/renombrar/retipar columnas) registradas por dataset.
 */
class SchemaEvolutionRegisterDao extends PostgreSqlManager[SchemaEvolutionRegister] with iSchemaEvolutionRegisterDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "schema_evolution_register"

  /**
   * Devuelve la acción de evolución de esquema con el id indicado.
   *
   * @param action_id id de la acción
   * @return la acción encontrada
   */
  def getActionByActionId(action_id: Int): SchemaEvolutionRegister = {
    val whereSql =
      s"""action_id = $action_id""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Devuelve las acciones de evolución de esquema registradas para un dataset por su id.
   *
   * @param dataset_id id del dataset
   * @return las acciones encontradas
   */
  def getActionsByDatasetId(dataset_id: Int): Seq[SchemaEvolutionRegister] = {
    val whereSql =
      s"""dataset_id = $dataset_id""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve las acciones de evolución de esquema registradas para un dataset por su nombre.
   *
   * @param dataset_name nombre del dataset
   * @return las acciones encontradas
   */
  def getActionsByDatasetName(dataset_name: String): Seq[SchemaEvolutionRegister] = {
    val whereSql =
      s"""dataset_name = '$dataset_name'""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Construye la SELECT completa sobre schema_evolution_register con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a SchemaEvolutionRegister.
   *
   * @param sql sentencia a ejecutar
   * @return las acciones resultantes
   */
  private def select(sql: String): Seq[SchemaEvolutionRegister] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo SchemaEvolutionRegister.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el SchemaEvolutionRegister construido a partir de la fila
   */
  override def mapperRowdata(resultSet: ResultSet): SchemaEvolutionRegister = {
    logger.info(s"Mapping SchemaEvolutionRegister Row: Action id ${resultSet.getString("action_id")}")

    SchemaEvolutionRegister(
      action_id = resultSet.getInt("action_id"),
      action_name = resultSet.getString("action_name"),
      dataset_id = resultSet.getInt("dataset_id"),
      dataset_name = resultSet.getString("dataset_name"),
      column = resultSet.getString("column"),
      new_name = resultSet.getString("new_name"),
      column_type = resultSet.getString("column_type"),
      landing_regeneration = resultSet.getBoolean("landing_regeneration"),
      registration_date = resultSet.getString("registration_date"),
      execution_date = resultSet.getString("execution_date")
    )
  }
}
