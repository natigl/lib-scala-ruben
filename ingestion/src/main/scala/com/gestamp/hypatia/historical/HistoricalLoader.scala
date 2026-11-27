package com.gestamp.hypatia.historical

import com.gestamp.hypatia.adls.ConstructorPaths.{buildRawPath, buildLandingRootPath, buildLandingRootPathCDC, getPathCheckpoints}
import com.gestamp.hypatia.databricks.DatabricksManager
import com.gestamp.hypatia.postgresql.model.Dataset
import org.apache.spark.sql.DataFrame

import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
 * Base de los cargadores históricos: operaciones comunes de limpieza (checkpoints,
 * ficheros de raw_sources) y descubrimiento de directorios de landing. Cada tipo
 * de extracción implementa su propia escritura en writeHistoricalDfToRawSource.
 */
trait HistoricalLoader {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val databricks = new DatabricksManager()
  private val rootPathCheckpoints = s"${getPathCheckpoints()}"

  /**
   * Devuelve los directorios (timestamps de extracción) de landing del dataset,
   * ordenados de más antiguo a más reciente.
   *
   * @param source  source del dataset
   * @param dataset nombre del dataset
   * @return los nombres de los directorios ordenados
   */
  def getSortedLandingDirectories(source: String, dataset: String): Seq[String] = {
    val landingRootPath = getLandingRootPath(source, dataset)
    Try {
      val directoriesInfo = databricks.dbUtilsFsLs(landingRootPath).filter(_.isDir)
      val directories: Seq[String] = directoriesInfo.map(_.path.split("/").last.toInt).sorted.map(_.toString)
      directories
      } match {
        case Success(directories) => directories
        case Failure(e) =>
          logger.error(s"Something went wrong when trying to get landing directories for table $dataset table", e.getMessage)
          throw new Exception(s"Error when trying to get landing directories for table $dataset table", e)
      }
    }

  /**
   * Devuelve la ruta raíz de landing del dataset; para SAP_ECC usa la variante
   * CDC (subcarpeta _checkpoints).
   *
   * @param source  source del dataset
   * @param dataset nombre del dataset
   * @return la ruta raíz de landing
   */
  private def getLandingRootPath(source: String, dataset: String): String = {
    val landingRootPath = if (source == "SAP_ECC") {
      buildLandingRootPathCDC(source, dataset)
    } else buildLandingRootPath(source, dataset)
    landingRootPath
  }

  /**
   * Borra el checkpoint del Auto Loader del dataset (conservando la carpeta
   * 'schema') para que la carga histórica reprocese todo desde el principio.
   *
   * @param source  source del dataset
   * @param dataset nombre del dataset
   */
  def deleteCheckpoint(source: String, dataset: String): Unit = {
    logger.info(s"Deleting autoloader checkpoint for $dataset table, excluding 'schema' folder")
    val pathCheckpoints = s"$rootPathCheckpoints/$source/$dataset"
    Try(databricks.dbUtilsFsLs(pathCheckpoints)) match {
      case Success(files) =>
        val toDelete = files.filterNot(fileInfo => {
          val folderName = fileInfo.path.stripSuffix("/").split("/").last
          folderName == "schema"
        })
        toDelete.foreach(fileInfo => {
          logger.info(s"Removing: ${fileInfo.path}")
          databricks.dbUtilsFsRm(fileInfo.path)
        })
      case Failure(exception) => logger.info(s"The path $pathCheckpoints was not found for some reason. " +
        s"No files found inside. Maybe the path didn't exist previously. The exception was the following: ${exception.getMessage}")
    }
  }

  /**
   * Borra los ficheros de datos del dataset en raw_sources conservando el
   * _delta_log (el historial de la tabla Delta se mantiene).
   *
   * @param source  source del dataset
   * @param dataset nombre del dataset
   */
  def deleteRawSourcesFiles(source: String, dataset: String): Unit = {
    logger.info(s"Deleting data files for $dataset table in raw_sources")
    val rawPath = buildRawPath(source, dataset)
    val files = databricks.dbUtilsFsLs(rawPath)

    val deltaFile = "_delta_log/"

    files.foreach { file =>
      val filePath = file.path
      if (!filePath.endsWith(deltaFile)) {
        databricks.dbUtilsFsRm(filePath)
      }
    }
  }
  /**
   * Escribe el histórico completo del dataset en raw_sources; cada tipo de
   * extracción (FULL, FULL_PARTITION, DELTA/CDC) define su estrategia.
   *
   * @param dataFrame   DataFrame con el histórico leído de landing
   * @param source      source del dataset
   * @param dataset     dataset a cargar
   * @param path        ruta física destino en raw_sources
   * @param directories directorios de landing a procesar, ordenados
   */
  def writeHistoricalDfToRawSource(dataFrame: DataFrame, source: String, dataset: Dataset, path: String, directories: Seq[String]): Unit
}
