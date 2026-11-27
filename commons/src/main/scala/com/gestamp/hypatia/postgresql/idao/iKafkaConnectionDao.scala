package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.KafkaConnection

/**
 * Contrato de consultas sobre kafka_connection_setting. Implementación: KafkaConnectionDao.
 */
trait iKafkaConnectionDao {

  /** Devuelve la configuración de conexión Kafka del source indicado. */
  def getConnectionConfigBySource(source_name: String): KafkaConnection

}
