package com.gestamp.hypatia.config

import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.adls.ConstructorPaths.getPathCheckpoints
import com.gestamp.hypatia.config
import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.utils.IngestionConstants
import org.slf4j.{Logger, LoggerFactory}

//TODO: Falta por hacer schema evolution y el particionado
class AutoLoaderConfigBuilder(dataset: Dataset, source: String) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val pathCheckpoints = s"${getPathCheckpoints()}/$source/${dataset.dataset}/schema"

  /**
   * Returns the Auto Loader configuration for a specific dataset and source, based on the dataset format.
   * Important: The order of the cases cannot be altered
   * @return A map with the Auto Loader configuration parameters.
   */
  def getAutoLoaderConfig(): Map[String, String] = {
    (dataset.format, source) match {
      case ("csv", IngestionConstants.SAP_ECC_SOURCE) => sapeccMode()
      case ("json", IngestionConstants.SAP_ECC_SOURCE) => sapeccMode()
      case ("parquet", IngestionConstants.SAP_SNP_SOURCE) => sapSnpMode()
      case ("parquet", IngestionConstants.SAP_ECC_SOURCE) => sapSnpMode()
      case ("csv", _) => csvMode()
      case ("text", _) => textMode()
      case ("json_multiline", _) => jsonMultilineMode()
      case (format, _)
        if List("bmp", "gif", "jpeg", "png").contains(format) => imageMode()
      case _ => defaultMode()
    }
  }

  /**
   * Returns the Auto Loader configuration for a default mode
   * @return A map with the Auto Loader configuration parameters.
   */
  private def defaultMode(): Map[String, String] = {
    logger.info(s"Auto Loader 'Default Mode' Configuration")
    Map[String, String](
      CloudFiles.FORMAT -> dataset.format,
      CloudFiles.SCHEMA_EVOLUTION_MODE -> "addNewColumns",
      CloudFiles.SCHEMA_LOCATION -> pathCheckpoints,
      CloudFiles.PATH_LANDING -> ConstructorPaths.buildLandingPath(source, dataset.dataset)
    )
  }

  /**
   * Returns the Auto Loader configuration for a SAPECC datasets.
   * @return A map with the Auto Loader configuration parameters.
   */
  private def sapeccMode(): Map[String, String] = {
    logger.info(s"Auto Loader 'Sap ECC' Configuration")
    Map[String, String](
      CloudFiles.FORMAT -> dataset.format,
      CloudFiles.SCHEMA_EVOLUTION_MODE -> "addNewColumns",
      CloudFiles.SCHEMA_LOCATION -> pathCheckpoints,
      CloudFiles.PATH_LANDING -> ConstructorPaths.buildLandingPathCDC(source, dataset.dataset),
      CloudFiles.RECURSIVEFILELOOKUP -> "true",
      CloudFiles.DELIMITER -> dataset.delimiter,
      CloudFiles.HEADER -> dataset.header.toString,
      CloudFiles.MULTILINE -> "true"
    )
  }

  /**
   * Returns the Auto Loader configuration for a SAPECC datasets.
   * @return A map with the Auto Loader configuration parameters.
   */
  private def sapSnpMode(): Map[String, String] = {
    logger.info(s"Auto Loader 'Sap SNP' Configuration")
    Map[String, String](
      CloudFiles.FORMAT -> dataset.format,
      CloudFiles.SCHEMA_EVOLUTION_MODE -> "addNewColumns",
      CloudFiles.SCHEMA_LOCATION -> pathCheckpoints,
      CloudFiles.PATH_LANDING -> ConstructorPaths.buildLandingPath(source, dataset.dataset),
      CloudFiles.RECURSIVEFILELOOKUP -> "true",
      CloudFiles.DELIMITER -> dataset.delimiter,
      CloudFiles.HEADER -> dataset.header.toString,
      CloudFiles.MULTILINE -> "true"
    )
  }

  /**
   * Returns the Auto Loader configuration for a CSV dataset.
   * @return A map with the Auto Loader configuration parameters.
   */
  private def csvMode(): Map[String, String] = {
    logger.info(s"Auto Loader 'CSV Format' Configuration")
    Map[String, String](
      CloudFiles.FORMAT -> dataset.format,
      CloudFiles.SCHEMA_EVOLUTION_MODE -> "addNewColumns",
      CloudFiles.SCHEMA_LOCATION -> pathCheckpoints,
      CloudFiles.PATH_LANDING -> ConstructorPaths.buildLandingPath(source, dataset.dataset),
      CloudFiles.DELIMITER -> dataset.delimiter,
      CloudFiles.HEADER -> dataset.header.toString,
      CloudFiles.MULTILINE -> "true"
    )
  }

  /**
   * Returns the Auto Loader configuration for a Text dataset.
   * @return A map with the Auto Loader configuration parameters.
   */
  private def textMode(): Map[String, String] = {
    logger.info(s"Auto Loader 'Text Format' Configuration")
    Map[String, String](
      CloudFiles.FORMAT -> dataset.format,
      CloudFiles.SCHEMA_EVOLUTION_MODE -> "addNewColumns",
      CloudFiles.SCHEMA_LOCATION -> pathCheckpoints,
      CloudFiles.PATH_LANDING -> ConstructorPaths.buildLandingPath(source, dataset.dataset)
    )
  }

  /**
   * Returns the Auto Loader configuration for an Image dataset.
   *
   * @return A map with the Auto Loader configuration parameters.
   */
  private def imageMode(): Map[String, String] = {
    logger.info(s"Auto Loader 'Image Format' Configuration")
    Map[String, String](
      CloudFiles.FORMAT -> "binaryFile",
      CloudFiles.SCHEMA_LOCATION -> pathCheckpoints,
      CloudFiles.PATHGLOBFILTER -> "*.".concat(dataset.format),
      CloudFiles.RECURSIVEFILELOOKUP -> "true",
      CloudFiles.PATH_LANDING -> ConstructorPaths.buildLandingPath(source, dataset.dataset)
    )
  }

  /**
   * Returns the Auto Loader configuration for JSON datasets where each file
   * is a single, complete JSON object spanning multiple lines (e.g. COCO annotations).
   *
   * Note: cloudFiles.format must be "json" (the Spark-recognized format),
   * not "json_multiline" (which is only the metadata.dataset.format identifier).
   *
   * @return A map with the Auto Loader configuration parameters.
   */
  private def jsonMultilineMode(): Map[String, String] = {
    logger.info(s"Auto Loader 'JSON Multiline' Configuration")
    Map[String, String](
      CloudFiles.FORMAT                -> "json",
      CloudFiles.MULTILINE             -> "true",
      CloudFiles.SCHEMA_EVOLUTION_MODE -> "addNewColumns",
      CloudFiles.SCHEMA_LOCATION       -> pathCheckpoints,
      CloudFiles.PATH_LANDING          -> ConstructorPaths.buildLandingPath(source, dataset.dataset)
    )
  }

}

object AutoLoaderConfigBuilder {
  /** Crea el builder de configuración del Auto Loader para el (dataset, source) dados. */
  def apply(dataset: Dataset, source: String): AutoLoaderConfigBuilder = new AutoLoaderConfigBuilder(dataset, source)
}