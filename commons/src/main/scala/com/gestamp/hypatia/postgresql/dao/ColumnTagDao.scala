package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iColumnTagDao
import com.gestamp.hypatia.postgresql.model.ColumnTag
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.column_tag: tags asociados a nombres de columna,
 * usados por la propagación de tags en Unity Catalog.
 */
class ColumnTagDao extends PostgreSqlManager[ColumnTag] with iColumnTagDao {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "column_tag"

  /**
   * Mapea una fila del ResultSet al modelo ColumnTag.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el ColumnTag construido a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): ColumnTag = {
    logger.info(s"Mapping ColumnTag row: ${resultSet.getString("column_name")}")
    ColumnTag(
      id          = resultSet.getLong("id"),
      column_name = resultSet.getString("column_name"),
      tag_name    = resultSet.getString("tag_name")
    )
  }

  /**
   * Devuelve todos los tags de columna definidos.
   *
   * @return todos los ColumnTag de la tabla
   */
  override def getAllColumnTags: Seq[ColumnTag] = {
    val sql = s"SELECT id, column_name, tag_name FROM ${schema + "." + table}"
    this.runSelect(sql)
  }
}
