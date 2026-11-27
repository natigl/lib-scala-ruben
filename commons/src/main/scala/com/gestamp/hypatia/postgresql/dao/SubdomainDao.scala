package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iSubdomainDao
import com.gestamp.hypatia.postgresql.model.Subdomain
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.subdomain: subdominios de la taxonomía del lago.
 */
class SubdomainDao extends PostgreSqlManager[Subdomain] with iSubdomainDao {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "subdomain"

  /**
   * Construye la SELECT completa sobre la tabla subdomain con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a Subdomain.
   *
   * @param sql sentencia a ejecutar
   * @return los subdominios resultantes
   */
  private def select(sql: String): Seq[Subdomain] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo Subdomain.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el Subdomain construido a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): Subdomain = {
    logger.info(s"Mapping Dataset Row: ${resultSet.getString("code")}")

    Subdomain(
      code = resultSet.getString("code"),
      description = resultSet.getString("description"),
      validated = resultSet.getBoolean("validated")
    )

  }

  /**
   * Devuelve el subdominio validado con el código indicado.
   *
   * @param code código del subdominio
   * @return el subdominio encontrado
   */
  def getSubdomainByCode(code: String): Subdomain = {
    val whereSql =
      s"""code = '$code' and validated = true""".stripMargin

    select(createSql(whereSql)).head
  }
}
