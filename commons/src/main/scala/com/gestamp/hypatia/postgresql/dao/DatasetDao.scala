package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iDatasetDao
import com.gestamp.hypatia.postgresql.model.Dataset
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.dataset: consultas de datasets activos por nombre, id
 * y por el par (nombre, source).
 */
class DatasetDao extends PostgreSqlManager[Dataset] with iDatasetDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "dataset"

  /**
   * Devuelve el primer dataset activo con ese nombre. OJO: el nombre no es único
   * (puede repetirse entre sources); si hay varios devuelve uno arbitrario. Para
   * resolver sin ambigüedad usar la sobrecarga con source.
   *
   * @param name nombre del dataset
   * @return el dataset activo encontrado
   */
  def getDatasetByName(name: String): Dataset = {
    val whereSql =
      s"""dataset = '$name' and active""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Devuelve los datasets activos cuyos nombres estén en la lista. Igual que
   * getDatasetByName, no desambigua nombres repetidos entre sources.
   *
   * @param names nombres de los datasets
   * @return los datasets activos encontrados
   */
  def getDatasetsBySequenceNames(names: Seq[String]): Seq[Dataset] = {
    val whereSql =
      s"""dataset in (${names.map(name => "'"+name+"'").mkString(",")}) and active""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Resuelve un dataset por (nombre, source). El nombre de dataset NO es único en la tabla dataset
   * (la clave única es (dataset, "group") y el nombre puede repetirse entre sources), así que buscar
   * solo por nombre puede devolver el dataset equivocado. La tabla dataset no tiene columna source:
   * la relación con el source se obtiene a través de las tablas de extracción (ver [[ExtractionTables]]),
   * que apuntan a dataset(id) con el par (source, dataset_id).
   *
   * @throws NoSuchElementException si no existe ningún dataset activo con ese nombre para ese source
   */
  def getDatasetByName(name: String, source: String): Dataset = {
    getDatasetsBySequenceNames(Seq(name), source).headOption.getOrElse(
      throw new NoSuchElementException(s"No active dataset '$name' found for source '$source'"))
  }

  /**
   * Versión por (nombres, source) de getDatasetsBySequenceNames: filtra además de por nombre por los
   * dataset_id asociados al source en las tablas de extracción, de forma que datasets con el mismo
   * nombre en sources distintos no se mezclen. Los nombres que no existan para ese source simplemente
   * no aparecen en el resultado (el llamante decide si eso es un error).
   */
  def getDatasetsBySequenceNames(names: Seq[String], source: String): Seq[Dataset] = {
    val whereSql =
      s"""dataset in (${names.map(name => "'"+name+"'").mkString(",")}) and active
         | and id in (${ExtractionTables.datasetIdsBySourceSql(schema, source)})""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve el dataset activo con el id indicado.
   *
   * @param id id del dataset
   * @return el dataset activo encontrado
   */
  def getDatasetById(id: Int): Dataset = {
    val whereSql =
      s"""id = $id and active""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Devuelve los datasets activos cuyos ids estén en la lista.
   *
   * @param ids ids de los datasets
   * @return los datasets activos encontrados
   */
  def getDatasetsBySequenceIds(ids: Seq[Int]): Seq[Dataset] = {
    val whereSql =
      s"""id in (${ids.mkString(",")}) and active""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Construye la SELECT completa sobre la tabla dataset con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a Dataset.
   *
   * @param sql sentencia a ejecutar
   * @return los datasets resultantes
   */
  private def select(sql: String): Seq[Dataset] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo Dataset.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el Dataset construido a partir de la fila
   */
  override def mapperRowdata(resultSet: ResultSet): Dataset = {
    // Se loguean id y group además del nombre: el nombre no identifica un dataset único
    // (puede repetirse entre sources), así que sin ellos no se distingue qué fila se ha traído.
    logger.info(s"Mapping Dataset Row: ${resultSet.getString("dataset")} " +
      s"(id=${resultSet.getInt("id")}, group=${resultSet.getString("group")})")

    Dataset(
      id = resultSet.getInt("id"),
      dataset = resultSet.getString("dataset"),
      extract_type = resultSet.getString("extract_type"),
      delta_column = resultSet.getString("delta_column"),
      format = resultSet.getString("format"),
      blocksizeinmb = resultSet.getInt("blocksizeinmb"),
      mappings = resultSet.getString("mappings"),
      header = resultSet.getBoolean("header"),
      trigger_id = resultSet.getInt("trigger_id"),
      active = resultSet.getBoolean("active"),
      group = resultSet.getString("group"),
      delimiter = resultSet.getString("delimiter"),
      key_columns = resultSet.getString("key_columns")
    )
  }
}
