package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iDistillationDao
import com.gestamp.hypatia.postgresql.model.Distillation
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.distillation: consultas de distillations (entidades
 * de la capa silver) por id, nombre y dominio/subdominio.
 */
class DistillationDao extends PostgreSqlManager[Distillation] with iDistillationDao {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "distillation"

  /**
   * Construye la SELECT completa sobre la tabla distillation con la condición WHERE dada.
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
   * Ejecuta la consulta y mapea las filas a Distillation.
   *
   * @param sql sentencia a ejecutar
   * @return las distillations resultantes
   */
  private def select(sql: String): Seq[Distillation] = {
    this.runSelect(sql)
  }


  /**
   * Mapea una fila del ResultSet al modelo Distillation.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la Distillation construida a partir de la fila
   */
  override protected def mapperRowdata(resultSet: ResultSet): Distillation = {
    logger.info(s"Mapping Distillation Row: ${resultSet.getString("datum")}")

    Distillation(
      id = resultSet.getInt("id"),
      datum = resultSet.getString("datum"),
      key_columns = resultSet.getString("key_columns"),
      ingest_type = resultSet.getString("ingest_type"),
      partition_by = resultSet.getString("partition_by")
    )
  }

  /**
   * Devuelve la distillation con el id indicado.
   *
   * @param id id de la distillation
   * @return la distillation encontrada
   */
  override def getDistillationById(id: Int): Distillation = {
    val whereSql =
      s"""id = $id""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Devuelve la distillation con el nombre (datum) indicado.
   *
   * @param name nombre de la distillation
   * @return la distillation encontrada
   */
  override def getDistillationByName(name: String): Distillation = {
    val whereSql =
      s"""datum = '$name'""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Devuelve las distillations cuyos ids estén en la lista.
   *
   * @param ids ids de las distillations
   * @return las distillations encontradas
   */
  override def getDistillationBySequenceIds(ids: Seq[Int]): Seq[Distillation] = {
    val whereSql =
      s"""id in (${ids.mkString(",")})""".stripMargin

    select(createSql(whereSql))
  }

  /**
   * Devuelve las distillations de un dominio y subdominio.
   *
   * @param domainCode    código del dominio
   * @param subdomainCode código del subdominio
   * @return las distillations encontradas
   */
  override def getDistillationByDomainSubdomain(domainCode: String, subdomainCode: String): Seq[Distillation] = {
    val whereSql = s"""domain_code = '$domainCode' and subdomain_code = '$subdomainCode'"""
    select(createSql(whereSql))
  }

  /**
   * Devuelve la distillation de un dominio/subdominio con el nombre de tabla dado,
   * o null si no existe.
   *
   * @param domainCode    código del dominio
   * @param subdomainCode código del subdominio
   * @param table         nombre (datum) de la distillation
   * @return la distillation encontrada, o null si no hay ninguna
   */
  override def getDistillationByDomainSubdomainAndTable(domainCode: String, subdomainCode: String, table: String): Distillation = {
    val whereSql = s"""domain_code = '$domainCode' and subdomain_code = '$subdomainCode' and datum = '$table'"""
    val seq = select(createSql(whereSql))
    if(seq.isEmpty){
      null
    }else{
      seq.head
    }

  }
}
