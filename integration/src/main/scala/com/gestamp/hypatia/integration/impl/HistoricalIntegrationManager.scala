package com.gestamp.hypatia.integration.impl

import com.gestamp.hypatia.adls.ADLS2Manager.sparkWriteData
import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.createDatabase
import com.gestamp.hypatia.databricks.deltalake.DeltaChangeDataFeed
import com.gestamp.hypatia.integration.args.HistoricalIntegrationJobParams
import com.gestamp.hypatia.integration.exception.HistoricalIntegrationManagerException
import com.gestamp.hypatia.integration.logic.IntegrationOperator
import com.gestamp.hypatia.integration.utils.Config
import com.gestamp.hypatia.integration.impl.IntegrationManager.writeDfToIntegration
import com.gestamp.hypatia.integration.utils.ConstantsIntegration
import com.gestamp.hypatia.postgresql.dao._
import com.gestamp.hypatia.postgresql.model._
import com.gestamp.hypatia.spark.SparkOptions
import com.gestamp.hypatia.utils.CommonsConstants
import com.gestamp.hypatia.utils.JsonTools.jsonToCaseClass
import com.gestamp.hypatia.integration.utils.SchemaCsvFileReader.castDataFrameByCSVSchema
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Left, Right, Success, Try}
import org.slf4j.{Logger, LoggerFactory}

object HistoricalIntegrationManager {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val distillationDao: DistillationDao = new DistillationDao
  val datasetDao: DatasetDao = new DatasetDao

  /**
   * Orquesta la integración histórica: para cada distillation implicada decide el
   * método de lectura/escritura según venga o no delta_version y según la taxonomía
   * de la relación, y procesa las distillations en paralelo. No se permite combinar
   * delta_version con taxonomías multi-dataset.
   *
   * @param jobParams parámetros del job histórico (source, relaciones y delta_version opcional)
   * @return mensaje de fin del proceso
   */
  def handleData(jobParams: HistoricalIntegrationJobParams): String = {
    logger.info(s"Start handle data Integration method")

    val distillationsToProcess: Seq[Int] = jobParams.distillations.map(datarelationship => datarelationship.distillation_id).distinct
    val distillations: Seq[Distillation] = distillationDao.getDistillationBySequenceIds(distillationsToProcess)

    distillations.par.foreach { dis =>
      logger.info(s"Start historical load process for the integration dataset: ${dis.datum}")
      val txDistOpt = jobParams.distillations.find(_.distillation_id == dis.id)
      val configOpt = txDistOpt.flatMap { tx =>
        Option(tx.operation).filter(_.nonEmpty).map { json =>
          logger.info(s"JSON taxonomy: $json")
          jsonToCaseClass[Config](json)
        }
      }

      val (readMethod, writeMethod) = (jobParams.delta_version, configOpt) match {
        case (Some(deltaId), Some(config)) if config.taxonomy.operation != "none" =>
          throw new HistoricalIntegrationManagerException(
            "It is not possible to process a taxonomy involving multiple datasets " +
              "(operation != 'none') if a specific delta version is provided for reading from raw_clean layer"
          )

        case (Some(deltaId), Some(config)) =>
          (() => readHistoricalDataFromRawCleanWithOperation(config, jobParams.source, dis, Some(deltaId)),
            (df: DataFrame) => writeDfToIntegration(df, jobParams.source, dis))

        case (Some(deltaId), None) =>
          val dataset = datasetDao.getDatasetById(txDistOpt.get.dataset_id)
          (() => readHistoricalDataVersionFromRawClean(jobParams.source, dataset.dataset, dataset.extract_type, deltaId),
            (df: DataFrame) => writeDfToIntegration(df, jobParams.source, dis))

        case (None, Some(config)) =>
          (() => readHistoricalDataFromRawCleanWithOperation(config, jobParams.source, dis),
            (df: DataFrame) => writeTotalHistoricalDfToIntegration(df, jobParams.source, dis))

        case (None, None) =>
          val dataset = datasetDao.getDatasetById(txDistOpt.get.dataset_id)
          (() => readTotalHistoricalDataFromRawClean(jobParams.source, dataset.dataset),
            (df: DataFrame) => writeTotalHistoricalDfToIntegration(df, jobParams.source, dis))
      }

      Try(readMethod()) match {
        case Success(Right(dfRawClean)) =>
          logger.info(s"The historical read of raw_clean layer has been successfully completed for the following distillation: ${dis.datum}")
          val dfRawCleanNewSchema = castDataFrameByCSVSchema(jobParams.source, dis.datum, dfRawClean)
          Try(writeMethod(dfRawCleanNewSchema)) match {
            case Success(Right(_)) => Right(())
            case Success(Left(e)) => throw new HistoricalIntegrationManagerException(
              s"Historical integration unexpected writing error: ${e.getMessage}\n",
              e)
            case Failure(e) => throw new HistoricalIntegrationManagerException(
              s"Historical integration user configuration writing error: ${e.getMessage}\n",
              e)
          }
          logger.info(s"Historical distillation was written: ${dis.datum}")
        case Success(Left(e)) =>
          throw new HistoricalIntegrationManagerException(s"It was not possible to read data from raw_clean layer." +
            s"For this reason, nothing was done to the ${dis.datum} table in integration layer. No data has been written." +
            s"The unexpected error was the following: ${e.getMessage}\n", e)
        case Failure(e) =>
          throw new HistoricalIntegrationManagerException(s"It was not possible to read data from raw_clean layer." +
            s"For this reason, nothing was done to the ${dis.datum} table in integration layer. No data has been written." +
            s"The user configuration error was the following: ${e.getMessage}\n", e)
      }
    }
    "End Handle Historical Data Integration"
  }

  /**
   * Reads total historical data from the raw/clean layer and returns a DataFrame
   * @param source      The name of the data source in the raw layer
   * @param datasetName The name of the dataset in the raw layer
   * @return A DataFrame representing the requested data
   * @throws IllegalArgumentException if the extraction type is invalid or if an error occurs while reading the data
   */
  private def readTotalHistoricalDataFromRawClean(source: String, datasetName: String): Either[Throwable, DataFrame] = {
    logger.info(s"Start method to read total historical data from raw/clean layer: $source and $datasetName")
    DeltaTablesManager.readDeltaTable(CommonsConstants.RAW_CLEAN, source, datasetName)
  }

  /**
   * Reads delta version from the raw/source layer and returns a DataFrame
   *
   * @param source       The name of the data source in the raw layer
   * @param datasetName  The name of the dataset in the raw layer
   * @param extractType  Extract type of the dataset
   * @param deltaVersion Delta version for reading
   * @return A DataFrame representing the requested data
   * @throws IllegalArgumentException if an error occurs while reading the data
   */
  private def readHistoricalDataVersionFromRawClean(source: String, datasetName: String, extractType: String, deltaVersion: Int): Either[Throwable, DataFrame] = {
    logger.info(s"Start method to read data from a specific historical version of raw_sources layer: $source and $datasetName")

    extractType.toUpperCase match {
      case ConstantsIntegration.FULL_PARTITION_EXTRACTION | ConstantsIntegration.DELTA_EXTRACTION | ConstantsIntegration.STREAM_EXTRACTION | ConstantsIntegration.FULL_EXTRACTION =>
        DeltaChangeDataFeed.readDeltaFeedFromVersion(CommonsConstants.RAW_CLEAN, source, datasetName, deltaVersion)
      case ConstantsIntegration.CDC_EXTRACTION =>
        DeltaChangeDataFeed.readDeltaFeedFromVersionCDC(CommonsConstants.RAW_CLEAN, source, datasetName, deltaVersion)
      case _ =>
        throw new IllegalArgumentException(s"Invalid extract type: $extractType")
    }
  }

  /**
   * Lee el histórico de raw_clean aplicando la taxonomía (union/join) en modo
   * carga histórica, opcionalmente sobre una versión Delta concreta.
   *
   * @param operation    configuración de la taxonomía
   * @param source       source de los datos
   * @param distillation distillation destino
   * @param deltaVersion versión Delta concreta a leer, si aplica
   * @return Right con el DataFrame resultante, o Left con el error
   */
  private def readHistoricalDataFromRawCleanWithOperation(operation: Config, source: String, distillation: Distillation,  deltaVersion: Option[Int] = None): Either[Throwable, DataFrame] = {
    logger.info(s"JSON taxonomy: $operation")
    Try(IntegrationOperator.applyOperation(operation, source, distillation, true, deltaVersion)) match {
      case Success(df) => Right(df)
      case Failure(e) => throw new HistoricalIntegrationManagerException(
        s"Taxonomy configuration error: ${e.getMessage}\n",
        e)
    }
  }

  /**
   * Writes a DataFrame to the "integration" layer, using the path and options specific to the provided dataset.
   * The data will be overwritten with total historical existing Delta data.
   * If necessary, the method creates the schema in the "integration" layer before writing the DataFrame.
   * @param dfRawCleanNewSchema the DataFrame to be written
   * @param source the source system or process associated with the DataFrame
   * @param dataset the dataset representing the metadata of the DataFrame
   * @throws IntegrationManagerException in case of any error
   */
  //TODO: Probar la sobreescritura dinámica, por particiones
  private def writeTotalHistoricalDfToIntegration(dfRawCleanNewSchema: DataFrame, source: String, dataset: Distillation): Either[Throwable, Unit] = {
    logger.info(s"Write DataFrame cleaned to 'Integration' layer: ${dataset.datum}")

    val dsIntegrationPath = ConstructorPaths.buildSilverIntegrationPath(s"$source", dataset.datum)

    createDatabase(s"${CommonsConstants.INTEGRATION}.$source") match {
      case Right(schemaName) =>
        logger.info(s"Write dataFrame cleaned, with total historical data, to 'Integration' layer: ${dataset.datum}")

        // Retorna una lista vacía en caso de que partition_by sea NULL en la BD
        val partitions = Option(dataset.partition_by)
          .map(_.split(",").map(_.trim).toList).getOrElse(List.empty[String])
        val overwriteSchemaOption = Map("overwriteSchema" -> "true")

        val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite,
          partitionBy = partitions, otherOptions = overwriteSchemaOption)

        sparkWriteData(s"${CommonsConstants.INTEGRATION}.$source.${dataset.datum}", dfRawCleanNewSchema,
          dsIntegrationPath, "saveAsTable", sparkOptions)

      case Left(e) =>
        throw new HistoricalIntegrationManagerException(s"Error creating schema: ${CommonsConstants.INTEGRATION}.$source.${dataset.datum}", e)
    }
  }

  /**
   * Marca el fin del job de integración histórica (solo log).
   *
   * @return mensaje de fin del job
   */
  def finishTasksHistoricalIntegration(): String = {
    logger.info("End Historical Integration Databricks Job")
    "End Historical Integration Databricks Job"
  }
}
