package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iUsecaseDao
import com.gestamp.hypatia.postgresql.model.Usecase
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.usecase: consultas de casos de uso por id, área,
 * programa y proceso de Hypatia (vía join con hypatia_dataset_manager).
 */
class UsecaseDao extends PostgreSqlManager[Usecase] with iUsecaseDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "usecase"
  private val table_hypatia_dataset_manager = "hypatia_dataset_manager"

  /**
   * Construye la SELECT de usecase con join a hypatia_dataset_manager, para
   * condiciones que filtran por columnas de esa tabla (alias dc).
   *
   * @param whereSql condición WHERE de la consulta
   * @return la sentencia SQL completa
   */
  protected def createSqlInnerJoin(whereSql: String): String = {
    s"""SELECT d.*
       | FROM ${schema+"."+table} d
       | INNER JOIN ${schema+"."+table_hypatia_dataset_manager} dc on dc.usecase_id = d.usecase_id
       | WHERE $whereSql""".stripMargin
  }

  /**
   * Construye la SELECT completa sobre la tabla usecase con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a Usecase.
   *
   * @param sql sentencia a ejecutar
   * @return los casos de uso resultantes
   */
  private def select(sql: String): Seq[Usecase] = {
    logger.info(s"about to run sql: $sql")
    this.runSelect(sql)
  }

  /**
   * Mapea una fila del ResultSet al modelo Usecase.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return el Usecase construido a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): Usecase = {
    logger.info(s"Mapping Usecase Row: ${resultSet.getString("name")}")

    Usecase(
      id = resultSet.getInt("id"),
      name = resultSet.getString("name"),
      usecase_id = resultSet.getString("usecase_id"),
      area = resultSet.getString("area"),
      program = resultSet.getString("program")
    )
  }

  /**
   * Devuelve el caso de uso con el id indicado.
   *
   * @param id id del caso de uso
   * @return el caso de uso encontrado
   */
  override def getUsecaseByUsecaseId(id: Int): Usecase = {
    val whereSql =
      s"""id = $id""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Devuelve los casos de uso de un área.
   *
   * @param area área a filtrar
   * @return los casos de uso encontrados
   */
  override def getUsecaseByArea(area: String): Seq[Usecase] = {
    val whereSql =
      s"""area = '$area'""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve los casos de uso de un programa.
   *
   * @param program programa a filtrar
   * @return los casos de uso encontrados
   */
  override def getUsecaseByProgram(program: String): Seq[Usecase] = {
    val whereSql =
      s"""program = '$program'""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve el caso de uso asociado a un proceso de Hypatia, resolviendo la
   * relación a través de hypatia_dataset_manager.
   *
   * @param hypt_process nombre del proceso de Hypatia
   * @return el caso de uso encontrado
   */
  override def getUsecaseByHyptProcess(hypt_process: String): Usecase = {
    val whereSql =
      s"""dc.hypt_process = '$hypt_process'""".stripMargin

    select(createSqlInnerJoin(whereSql)).head
  }
}
