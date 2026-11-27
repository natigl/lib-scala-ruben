package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iTagExclusionDao
import com.gestamp.hypatia.postgresql.model.TagExclusion
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.tag_exclusion: objetos (tablas/vistas) excluidos de
 * la propagación de tags de columnas.
 */
class TagExclusionDao extends PostgreSqlManager[TagExclusion] with iTagExclusionDao {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "tag_exclusion"

  /**
   * Mapea una fila del ResultSet al modelo TagExclusion.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la TagExclusion construida a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): TagExclusion = {
    logger.info(s"Mapping TagExclusion row: ${resultSet.getString("domain_code")}/${resultSet.getString("subdomain_code")}/${resultSet.getString("table_name")} (${resultSet.getString("object_type")})")
    TagExclusion(
      id             = resultSet.getLong("id"),
      domain_code    = resultSet.getString("domain_code"),
      subdomain_code = resultSet.getString("subdomain_code"),
      table_name     = resultSet.getString("table_name"),
      object_type    = resultSet.getString("object_type")
    )
  }

  /**
   * Devuelve las exclusiones de tags activas.
   *
   * @return las exclusiones activas
   */
  override def getActiveExclusions: Seq[TagExclusion] = {
    val sql = s"SELECT id, domain_code, subdomain_code, table_name, object_type FROM ${schema + "." + table} WHERE active = TRUE"
    this.runSelect(sql)
  }
}
