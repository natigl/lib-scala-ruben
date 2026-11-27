package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iBlobExtractionDao
import com.gestamp.hypatia.postgresql.model.BlobExtraction
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.blob_extraction: configuración de extracción desde
 * blob storage asociada a cada dataset vía el par (source, dataset_id).
 */
class BlobExtractionDao extends PostgreSqlManager[BlobExtraction] with iBlobExtractionDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "blob_extraction"

  /**
   * Devuelve la primera extracción activa cuyo dataset tenga el nombre indicado.
   *
   * @param name nombre del dataset
   * @return la extracción activa encontrada
   */
  def getBlobExtractionByName(name: String): BlobExtraction = {
    val whereSql =
      s"""dataset = '$name' and active""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Devuelve las extracciones activas cuyos datasets tengan alguno de los nombres dados.
   *
   * @param names nombres de los datasets
   * @return las extracciones activas encontradas
   */
  def getBlobExtractionBySequenceNames(names: Seq[String]): Seq[BlobExtraction] = {
    val whereSql =
      s"""dataset in (${names.map(name => "'"+name+"'").mkString(",")}) and active""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve la extracción activa con el id indicado.
   *
   * @param id id de la extracción
   * @return la extracción activa encontrada
   */
  def getBlobExtractionById(id: Int): BlobExtraction = {
    val whereSql =
      s"""id = $id and active""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Devuelve las extracciones activas asociadas a los dataset_id dados.
   *
   * @param dataset_ids ids de dataset
   * @return las extracciones activas encontradas
   */
  def getBlobExtractionBySequenceIds(dataset_ids: Seq[Int]): Seq[BlobExtraction] = {
    val whereSql =
      s"""dataset_id in (${dataset_ids.mkString(",")}) and active""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Construye la SELECT completa sobre blob_extraction con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a BlobExtraction.
   *
   * @param sql sentencia a ejecutar
   * @return las extracciones resultantes
   */
  private def select(sql: String): Seq[BlobExtraction] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo BlobExtraction.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la BlobExtraction construida a partir de la fila
   */
  override def mapperRowdata(resultSet: ResultSet): BlobExtraction = {
    logger.info(s"Mapping Dataset Row: ${resultSet.getString("blobextraction")}")

    BlobExtraction(
      id = resultSet.getInt("id"),
      source = resultSet.getString("source"),
      dataset_id = resultSet.getInt("dataset_id"),
      minidle = resultSet.getInt("minidle"),
      maxworkers = resultSet.getInt("maxworkers"),
      poolname = resultSet.getString("poolname"),
      path = resultSet.getString("path"),
      active = resultSet.getBoolean("active")
    )
  }
}


