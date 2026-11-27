package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.KafkaExtraction

/**
 * Contrato de consultas sobre kafka_extraction. Implementación: KafkaExtractionDao.
 */
trait iKafkaExtractionDao {

  /** Devuelve la configuración de extracción Kafka del dataset con ese id. */
  def getExtractionConfigByDataset(datasetId: Int): KafkaExtraction


}
