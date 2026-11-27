package com.gestamp.hypatia.config

import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.utils.IngestionConstants
import org.slf4j.{Logger, LoggerFactory}

class HistoricalLoaderConfigBuilder(dataset: Dataset, source: String) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)


  /**
   * Returns the Auto Loader configuration for a specific dataset and source, based on the dataset format.
   * Important: The order of the cases cannot be altered
   * @return A map with the Auto Loader configuration parameters.
   */
  def getHistoricalLoaderConfig(): Map[String, String] = {
    (dataset.format, source) match {
      case (_, IngestionConstants.SAP_ECC_SOURCE) => sapeccMode()
      case ("csv", _) => csvMode()
      case _ => Map.empty[String, String]
    }
  }

  /**
   * Returns the Historical Loader configuration for a SAP_ECC datasets.
   * @return A map with the Historical Loader configuration parameters.
   */
  private def sapeccMode(): Map[String, String] = {
    logger.info(s"Auto Loader 'Sap ECC' Configuration")
    Map[String, String](
      CloudFiles.DELIMITER -> dataset.delimiter,
      CloudFiles.HEADER -> dataset.header.toString,
      CloudFiles.MULTILINE -> "true"
    )
  }

  /**
   * Returns the Historical Loader configuration for a CSV dataset.
   * @return A map with the Historical Loader configuration parameters.
   */
  private def csvMode(): Map[String, String] = {
    logger.info(s"Historical Loader 'CSV Format' Configuration")
    Map[String, String](
      CloudFiles.DELIMITER -> dataset.delimiter,
      CloudFiles.HEADER -> dataset.header.toString,
      CloudFiles.MULTILINE -> "true"
    )
  }

}

object HistoricalLoaderConfigBuilder {
  /** Crea el builder de configuración del cargador histórico para el (dataset, source) dados. */
  def apply(dataset: Dataset, source: String): HistoricalLoaderConfigBuilder = new HistoricalLoaderConfigBuilder(dataset, source)
}
