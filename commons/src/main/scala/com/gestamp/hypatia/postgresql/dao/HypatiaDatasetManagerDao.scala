package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iHypatiaDatasetManagerDao
import com.gestamp.hypatia.postgresql.model.HypatiaDatasetManager
import com.gestamp.hypatia.utils.JsonTools
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.hypatia_dataset_manager: configuración de los procesos
 * de Hypatia (librería, clase principal, workers y parámetros custom por proceso).
 */
class HypatiaDatasetManagerDao extends PostgreSqlManager[HypatiaDatasetManager] with iHypatiaDatasetManagerDao {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "hypatia_dataset_manager"

  /**
   * Construye la SELECT completa sobre hypatia_dataset_manager con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a HypatiaDatasetManager.
   *
   * @param sql sentencia a ejecutar
   * @return las configuraciones resultantes
   */
  private def select(sql: String): Seq[HypatiaDatasetManager] = {
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo HypatiaDatasetManager, parseando
   * los parámetros custom desde JSON.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el HypatiaDatasetManager construido a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): HypatiaDatasetManager = {
    logger.info(s"Mapping HypatIA Dataset Manager Row: ${resultSet.getString("hypt_process")}")

    HypatiaDatasetManager(
      hypt_process = resultSet.getString("hypt_process"),
      library = resultSet.getString("library"),
      main_class = resultSet.getString("main_class"),
      min_workers = resultSet.getInt("min_workers"),
      max_workers = resultSet.getInt("max_workers"),
      custom_params = JsonTools.parseJsonParameters(resultSet.getString("custom_params")),
      usecase_id = resultSet.getString("usecase_id")
    )
  }

  /**
   * Devuelve la configuración del proceso de Hypatia indicado.
   *
   * @param hypt_process nombre del proceso
   * @return la configuración encontrada
   */
  override def getHyptManagerByProcess(hypt_process: String): HypatiaDatasetManager = {
    val whereSql =
      s"""hypt_process = '$hypt_process'""".stripMargin

    select(createSql(whereSql)).head
  }

/*  override def getHyptManagerbyUseCase(usecase_id: Integer): Seq[HyptManager] = {
    val whereSql =
      s"""usecase_id = $usecase_id""".stripMargin

    select(createSql(whereSql))
  }

  override def getHyptManagerByGroupId(group_id: Integer): Seq[HyptManager] = {
    val whereSql =
      s"""group_id = $group_id""".stripMargin

    select(createSql(whereSql))
  }

  override def getHyptManagerByLibrary(library: String): Seq[HyptManager] = {
    val whereSql =
      s"""library = $library""".stripMargin

    select(createSql(whereSql))
  }*/

}
