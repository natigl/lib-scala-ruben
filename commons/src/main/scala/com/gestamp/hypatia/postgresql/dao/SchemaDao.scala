package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iSchemaDao
import com.gestamp.hypatia.postgresql.model.Schema
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.schema: esquemas del lago y la capa a la que pertenecen.
 */
class SchemaDao extends PostgreSqlManager[Schema] with iSchemaDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "schema"

  /**
   * Construye la SELECT completa sobre la tabla schema con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a Schema.
   *
   * @param sql sentencia a ejecutar
   * @return los esquemas resultantes
   */
  private def select(sql: String): Seq[Schema] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo Schema.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el Schema construido a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): Schema = {
    logger.info(s"Mapping Schema Row: ${resultSet.getString("id")}")

    Schema(
      id = resultSet.getInt("id"),
      schema = resultSet.getString("schema"),
      layer = resultSet.getString("layer")
    )
  }

  /**
   * Devuelve el esquema con el id indicado.
   *
   * @param id id del esquema
   * @return el esquema encontrado
   */
  override def getSchemaById(id: Int): Schema = {
    val whereSql =
      s"""id = $id""".stripMargin

    select(createSql(whereSql)).head
  }
}
