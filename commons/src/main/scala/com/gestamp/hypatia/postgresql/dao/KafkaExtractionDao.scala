package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import com.gestamp.hypatia.postgresql.idao.iKafkaExtractionDao
import com.gestamp.hypatia.postgresql.model.{Dataset, KafkaExtraction}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet

/**
 * DAO de la tabla metadata.kafka_extraction: configuración de extracción desde
 * Kafka (topic, esquema, offsets) asociada a cada dataset.
 */
class KafkaExtractionDao extends PostgreSqlManager[KafkaExtraction] with iKafkaExtractionDao {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val table = "kafka_extraction"

  /**
   * Devuelve la primera configuración de extracción Kafka del dataset indicado.
   *
   * @param datasetId id del dataset
   * @return la configuración de extracción encontrada
   */
  def getExtractionConfigByDataset(datasetId: Int): KafkaExtraction = {

    val whereSql =
      s"""dataset_id = $datasetId order by id""".stripMargin

    select(createSql(whereSql)).head
  }

  /**
   * Ejecuta la consulta y mapea las filas a KafkaExtraction.
   *
   * @param sql sentencia a ejecutar
   * @return las configuraciones resultantes
   */
    private def select(sql: String): Seq[KafkaExtraction] = {
    this.runSelect(sql)
  }

  /**
   * Construye la SELECT completa sobre kafka_extraction con la condición WHERE dada.
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
   * Mapea una fila del ResultSet al modelo KafkaExtraction.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la KafkaExtraction construida a partir de la fila
   */
  override def mapperRowdata(resultSet: ResultSet): KafkaExtraction = {
    logger.info(s"Mapping Dataset Row: ${resultSet.getString("id")}")

    KafkaExtraction(
      topic = resultSet.getString("topic"),
      schema = resultSet.getString("schema"),
      startingOffset = resultSet.getString("starting_offset"),
      isToLand = resultSet.getBoolean("istoland"),
      maxOffsetsPerTrigger = resultSet.getInt("maxoffsetspertrigger")
    )
  }
}
