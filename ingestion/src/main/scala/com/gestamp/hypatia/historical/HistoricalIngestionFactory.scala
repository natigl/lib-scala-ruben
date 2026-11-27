package com.gestamp.hypatia.historical

import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.utils.IngestionConstants.{FULL_EXTRACTION,DELTA_EXTRACTION,FULL_PARTITION_EXTRACTION,CDC_EXTRACTION}

/**
 * Factoría que resuelve el cargador histórico adecuado según el tipo de extracción del dataset.
 */
object HistoricalIngestionFactory {

  /**
   * Devuelve el HistoricalLoader correspondiente al extract_type del dataset:
   * FULL, FULL_PARTITION o DELTA/CDC.
   *
   * @param dataset dataset a cargar
   * @return el cargador histórico adecuado
   * @throws Exception si el tipo de extracción no está soportado
   */
  def createHistoricalLoader(dataset: Dataset): HistoricalLoader = {
    dataset.extract_type match {
      case FULL_EXTRACTION => new FullHistoricalLoader()
      case DELTA_EXTRACTION => new DeltaCDCHistoricalLoader()
      case FULL_PARTITION_EXTRACTION => new FullPartitionHistoricalLoader()
      case CDC_EXTRACTION => new DeltaCDCHistoricalLoader()
      case _ => throw new Exception("""Invalid extract type to execute historical ingestion: the possible values are "FULL", "DELTA", "FULL_PARTITION" or "CDC" (Nothing was done)""")
    }
  }
}
