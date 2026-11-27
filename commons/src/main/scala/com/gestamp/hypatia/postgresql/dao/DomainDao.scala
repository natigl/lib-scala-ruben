package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iDomainsDao
import com.gestamp.hypatia.postgresql.model.Domain
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.domain: dominios de la taxonomía del lago.
 */
class DomainDao extends PostgreSqlManager[Domain] with iDomainsDao {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "domain"

  /**
   * Construye la SELECT completa sobre la tabla domain con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a Domain.
   *
   * @param sql sentencia a ejecutar
   * @return los dominios resultantes
   */
  private def select(sql: String): Seq[Domain] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo Domain.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el Domain construido a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): Domain = {
    logger.info(s"Mapping Dataset Row: ${resultSet.getString("code")}")

    Domain(
      code = resultSet.getString("code"),
      description = resultSet.getString("description"),
      validated = resultSet.getBoolean("validated")
    )

  }

  /**
   * Devuelve el dominio validado con el código indicado.
   *
   * @param code código del dominio
   * @return el dominio encontrado
   */
  def getDomainByCode(code: String): Domain = {
    val whereSql =
      s"""code = '$code' and validated = true""".stripMargin

    select(createSql(whereSql)).head
  }
}
