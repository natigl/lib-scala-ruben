package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iDataRelationshipDao
import com.gestamp.hypatia.postgresql.model.DataRelationship
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.data_relationship: relaciones dataset (bronze) →
 * distillation (silver) con la operación y campos de la transformación.
 */
class DataRelationshipDao extends PostgreSqlManager[DataRelationship] with iDataRelationshipDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "data_relationship"

  /**
   * Construye la SELECT completa sobre data_relationship con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a DataRelationship.
   *
   * @param sql sentencia a ejecutar
   * @return las relaciones resultantes
   */
  private def select(sql: String): Seq[DataRelationship] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo DataRelationship.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el DataRelationship construido a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): DataRelationship = {
    logger.info(s"Mapping DataRelationship Row: ${resultSet.getString("distillation_name")}")

    DataRelationship (
      dataset_id = resultSet.getInt("dataset_id"),
      dataset_name = resultSet.getString("dataset_name"),
      distillation_id = resultSet.getInt("distillation_id"),
      distillation_name = resultSet.getString("distillation_name"),
      operation = resultSet.getString("operation"),
      fields = resultSet.getString("fields"),
      select = resultSet.getString("select")
    )
  }

  /**
   * Devuelve las relaciones cuyas distillations tengan alguno de los nombres dados.
   *
   * @param distillation_names nombres de distillation
   * @return las relaciones encontradas
   */
  override def getDataRelationshipByDistillationName(distillation_names: Seq[String]): Seq[DataRelationship] = {
    val whereSql =
      s"""distillation_name in ('${distillation_names.mkString("','")}')""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve las relaciones de una distillation por su id.
   *
   * @param distillation_id id de la distillation
   * @return las relaciones encontradas
   */
  override def getDataRelationshipByDistillationId(distillation_id: Int): Seq[DataRelationship] = {
    val whereSql =
      s"""distillation_id = $distillation_id""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve las relaciones de las distillations cuyos ids estén en la lista.
   *
   * @param distillation_ids ids de distillation
   * @return las relaciones encontradas
   */
  override def getDataRelationshipBySeqDistillationId(distillation_ids: Seq[Int]): Seq[DataRelationship] = {
    val whereSql =
      s"""distillation_id in (${distillation_ids.mkString(",")})""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve las relaciones de los datasets cuyos ids estén en la lista,
   * ordenadas por dataset y distillation.
   *
   * @param dataset_ids ids de dataset
   * @return las relaciones encontradas
   */
  override def getDataRelationshipBySeqDatasetId(dataset_ids: Seq[Int]): Seq[DataRelationship] = {
    val whereSql =
      s"""dataset_id in (${dataset_ids.mkString(",")}) order by dataset_id, distillation_id""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve las relaciones de los datasets con los nombres dados. OJO: el nombre de
   * dataset puede repetirse entre sources; usar la sobrecarga con source para desambiguar.
   *
   * @param dataset_names nombres de dataset
   * @return las relaciones encontradas
   */
  override def getDataRelationshipByDatasetName(dataset_names: Seq[String]): Seq[DataRelationship] = {
    val whereSql =
      s"""dataset_name in ('${dataset_names.mkString("','")}')""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Versión por (nombres, source): dataset_name no identifica un dataset único (el mismo nombre puede
   * existir en varios sources), así que se filtra además por los dataset_id asociados al source en las
   * tablas de extracción (ver [[ExtractionTables]]) para no traer distillations de otro source.
   */
  override def getDataRelationshipByDatasetName(dataset_names: Seq[String], source: String): Seq[DataRelationship] = {
    val whereSql =
      s"""dataset_name in ('${dataset_names.mkString("','")}')
         | and dataset_id in (${ExtractionTables.datasetIdsBySourceSql(schema, source)})""".stripMargin

    select(createSql(whereSql))
  }
}
