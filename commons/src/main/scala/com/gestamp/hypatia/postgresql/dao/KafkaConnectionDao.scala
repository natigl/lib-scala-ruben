package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iKafkaConnectionDao
import com.gestamp.hypatia.postgresql.model.{Dataset, KafkaConnection}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.kafka_connection_setting: datos de conexión al
 * broker de Kafka por source.
 */
class KafkaConnectionDao extends PostgreSqlManager[KafkaConnection] with iKafkaConnectionDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "kafka_connection_setting"


  /**
   * Devuelve la configuración de conexión Kafka del source indicado.
   *
   * @param source_name nombre del source
   * @return la conexión encontrada
   */
    def getConnectionConfigBySource(source_name: String): KafkaConnection = {
    val whereSql =
      s"""source = '$source_name'""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Ejecuta la consulta y mapea las filas a KafkaConnection.
   *
   * @param sql sentencia a ejecutar
   * @return las conexiones resultantes
   */
    private def select(sql: String): Seq[KafkaConnection] = {
    this.runSelect(sql)
  }

  /**
   * Construye la SELECT completa sobre kafka_connection_setting con la condición WHERE dada.
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
   * Mapea una fila del ResultSet al modelo KafkaConnection.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la KafkaConnection construida a partir de la fila
   */
  override def mapperRowdata(resultSet: ResultSet): KafkaConnection = {
    logger.info(s"Mapping Dataset Row: ${resultSet.getString("id")}")
    KafkaConnection(
      server = resultSet.getString("bootstrap_servers"),
    )
  }
}
