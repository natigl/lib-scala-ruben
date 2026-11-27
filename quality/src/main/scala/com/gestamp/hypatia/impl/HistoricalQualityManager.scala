package com.gestamp.hypatia.impl

import com.gestamp.hypatia.adls.ADLS2Manager.sparkWriteData
import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.args.HistoricalQualityJobParams
import com.gestamp.hypatia.databricks.deltalake.{DeltaChangeDataFeed, DeltaTablesManager}
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.createDatabase
import com.gestamp.hypatia.exceptions.QualityDefaultException
import com.gestamp.hypatia.logic.{QualityDefault, QualityJSON, QualitySAP}
import com.gestamp.hypatia.postgresql.dao.QualityDao
import com.gestamp.hypatia.postgresql.model.{Dataset, Quality}
import com.gestamp.hypatia.impl.QualityManager.writeDfToRawClean
import com.gestamp.hypatia.spark.SparkOptions
import com.gestamp.hypatia.utils.Utils.dropCDFcolumns
import com.gestamp.hypatia.utils.{CommonsConstants, ConstantsQuality}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.{Logger, LoggerFactory}
import com.gestamp.hypatia.exceptions.HistoricalQualityManagerException

import scala.util.{Failure, Left, Right, Success, Try}


object HistoricalQualityManager {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val qualityDefault = new QualityDefault {}
  val qualitySAP = new QualitySAP {}
  val qualityJson = new QualityJSON {}

  /**
   * Orquesta la calidad histórica: según venga o no delta_version, relee una versión
   * concreta o todo el histórico de raw_sources, aplica las reglas de calidad y
   * escribe en raw_clean, procesando los datasets en paralelo.
   *
   * @param jobParams parámetros del job histórico (source, datasets y delta_version opcional)
   * @return mensaje de fin del proceso
   */
  def handleData(jobParams: HistoricalQualityJobParams): String = {
    logger.info(s"Start to handle historical quality, datasets: ${jobParams.datasets.toString}")

    val (readMethod, writeMethod) = jobParams.delta_version match {
      case Some(id) => (
        (dataset: String, extractType: String) => readHistoricalDataVersionFromRawSource(jobParams.source, dataset, extractType,  id),
        (df: DataFrame, ds: Dataset) => writeDfToRawClean(df, jobParams.source, ds)
      )
      case None => (
        (dataset: String, extractType: String) => readTotalHistoricalDataFromRawSource(jobParams.source, dataset),
        (df: DataFrame, ds: Dataset) => writeTotalHistoricalDfToRawClean(df, jobParams.source, ds)
      )
    }

    jobParams.datasets.par.foreach { ds =>
      logger.info(s"Start historical load process for the raw_clean dataset: ${ds.dataset}")
      Try(readMethod(ds.dataset, ds.extract_type)) match {
        case Success(Right(dfRawSource)) =>
          logger.info(s"The historical read of raw_sources layer has been successfully completed for the following dataset: ${ds.dataset}")
          val qualityDao: QualityDao = new QualityDao
          val qualityRulesDao: Seq[Quality] = qualityDao.getRulesById(ds.id)

          val dfCleaned = QualityManager.applyQualityRules(dfRawSource, qualityRulesDao)
          logger.info(s"Schema of the dataset ${ds.dataset} cleaned is: ${dfCleaned.schema.toString}")

          Try(writeMethod(dfCleaned, ds)) match {
            case Success(Right(_)) => Right(())
            case Success(Left(e)) => throw new HistoricalQualityManagerException(
              s"Historical quality unexpected writing error: ${e.getMessage}\n", e)
            case Failure(e) => throw new HistoricalQualityManagerException(
              s"Historical quality user configuration writing error: ${e.getMessage}\n", e)
          }
          logger.info(s"Historical cleaned dataset was written: ${ds.dataset}")

        case Success(Left(e)) =>
          throw new HistoricalQualityManagerException(s"It was not possible to read table ${ds.dataset} from raw_sources layer. " +
            s"For this reason, nothing was done to the ${ds.dataset} table in raw_clean layer. No data has been written. " +
            s"The unexpected error was the following: ${e.getMessage}\n", e)

        case Failure(e) =>
          throw new HistoricalQualityManagerException(s"It was not possible to read table ${ds.dataset} from raw_sources layer. " +
            s"For this reason, nothing was done to the ${ds.dataset} table in raw_clean layer. No data has been written. " +
            s"The user configuration error was the following: ${e.getMessage}\n", e)
      }
    }
    "End Handle Historical Data Quality"
  }

  /**
   * Reads total historical data from the raw/source layer and returns a DataFrame
   * @param source      The name of the data source in the raw layer
   * @param datasetName The name of the dataset in the raw layer
   * @return A DataFrame representing the requested data
   * @throws IllegalArgumentException if an error occurs while reading the data
   */
  private def readTotalHistoricalDataFromRawSource(source: String, datasetName: String): Either[Throwable, DataFrame] = {
    logger.info(s"Start method to read total historical data from raw_sources layer: $source and $datasetName")
    DeltaTablesManager.readDeltaTable(CommonsConstants.RAW_SOURCES, source, datasetName)
  }

  /**
   * Reads delta version from the raw/source layer and returns a DataFrame
   *
   * @param source      The name of the data source in the raw layer
   * @param datasetName The name of the dataset in the raw layer
   * @param extractType Extract type of the dataset
   * @param deltaVersion Delta version for reading
   * @return A DataFrame representing the requested data
   * @throws IllegalArgumentException if an error occurs while reading the data
   */
  private def readHistoricalDataVersionFromRawSource(source: String, datasetName: String, extractType: String, deltaVersion: Int): Either[Throwable, DataFrame] = {
    logger.info(s"Start method to read data from a specific delta version of raw_sources layer: $datasetName table from $source source")

    extractType.toUpperCase match {
      case ConstantsQuality.FULL_PARTITION_EXTRACTION | ConstantsQuality.DELTA_EXTRACTION | ConstantsQuality.STREAM_EXTRACTION | ConstantsQuality.FULL_EXTRACTION =>
        DeltaChangeDataFeed.readDeltaFeedFromVersion(CommonsConstants.RAW_SOURCES, source, datasetName, deltaVersion)
      case ConstantsQuality.CDC_EXTRACTION =>
        DeltaChangeDataFeed.readDeltaFeedFromVersionCDC(CommonsConstants.RAW_SOURCES, source, datasetName, deltaVersion)
      case _ =>
        throw new IllegalArgumentException(s"Invalid extract type: $extractType")
    }
  }

  /**
   * Writes a DataFrame to the "raw clean" layer, using the path and options specific to the provided dataset.
   * The data will be overwritten with total historical existing Delta data.
   * If necessary, the method creates the schema in the "integration" layer before writing the DataFrame.
   * @param df the DataFrame to be written
   * @param source the source system or process associated with the DataFrame
   * @param dataset the dataset representing the metadata of the DataFrame
   * @throws QualityDefaultException in case of any error
   */
  private def writeTotalHistoricalDfToRawClean(df: DataFrame, source: String, dataset: Dataset): Either[Throwable, Unit] = {
    logger.info(s"Write total historical dataframe cleaned to 'raw clean' layer: ${dataset.dataset}")

    val dsRawCleanPath = ConstructorPaths.buildRawCleanPath(s"$source", dataset.dataset)
    val partitions = Option(dataset.delta_column)
      .map(_.split(",").map(_.trim).toList).getOrElse(List.empty[String])

    createDatabase(s"${CommonsConstants.RAW_CLEAN}.$source") match {
      case Right(schemaName) =>
        logger.info(s"Write dataFrame cleaned, with total historical data, to 'raw clean' layer: ${dataset.dataset}")
        val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite, partitionBy = partitions)
        sparkWriteData(s"${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", df, dsRawCleanPath, "saveAsTable", sparkOptions)
      case Left(exception) =>
        throw new QualityDefaultException(s"Error creating schema: ${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", exception)
    }
  }
  /**
   * Marca el fin del job de calidad histórica (solo log).
   *
   * @return mensaje de fin del job
   */
  def finishTasksHistoricalQuality(): String = {
    logger.info("End Historical Quality Databricks Job")
    "End Historical Quality Databricks Job"
  }
}

