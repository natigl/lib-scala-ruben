package com.gestamp.hypatia.impl

import com.gestamp.hypatia.adls.ADLS2Manager.sparkWriteData
import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.args.QualityJobParams
import com.gestamp.hypatia.databricks.deltalake.DeltaChangeDataFeed.upsertDeltaTableChangeDataCapture
import com.gestamp.hypatia.databricks.deltalake.{DeltaChangeDataFeed, DeltaTablesManager}
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.createDatabase
import com.gestamp.hypatia.exceptions.QualityDefaultException
import com.gestamp.hypatia.logic.{QualityDefault, QualityJSON, QualitySAP}
import com.gestamp.hypatia.postgresql.dao.{QualityDao, WorkflowDatasetExecutionDao, WorkflowTaskExecutionDao, LatestProcessedVersionsDao}
import com.gestamp.hypatia.exceptions.QualityManagerException
import com.gestamp.hypatia.postgresql.model.{Dataset, Quality, WorkflowDatasetExecution, LatestProcessedVersions}
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import com.gestamp.hypatia.utils.{CommonsConstants, ConstantsQuality, IngestionTaskTags}
import com.gestamp.hypatia.utils.WorkflowTags.jobRunID
import com.gestamp.hypatia.utils.QualityTaskTags.{taskKey, taskRunId}
import io.delta.tables._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentLinkedQueue
import scala.util.{Failure, Left, Right, Success, Try}


object QualityManager {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val qualityDao: QualityDao = new QualityDao
  private val datasetExecutionDao: WorkflowDatasetExecutionDao = new WorkflowDatasetExecutionDao
  private val workflowTaskExecutionDao: WorkflowTaskExecutionDao = new WorkflowTaskExecutionDao
  private val latestProcessedVersionsDao: LatestProcessedVersionsDao = new LatestProcessedVersionsDao

  private val qualityDefault = new QualityDefault {}
  private val qualitySAP = new QualitySAP {}
  private val qualityJson = new QualityJSON {}

  /**
   * Orquesta el proceso de calidad: filtra los datasets a los que salieron bien en la
   * task previa de Autoloader del mismo job (si existe), procesa cada uno en paralelo
   * (lectura de raw_sources, aplicación de reglas, escritura en raw_clean) y registra
   * la ejecución de cada dataset en orchestration.
   *
   * @param jobParams parámetros del job (source y datasets ya resueltos)
   * @return los nombres de los datasets procesados correctamente
   */
  def handleData(jobParams: QualityJobParams): List[String] = {
    logger.info(s"Data handling is started in the quality process. Datasets: ${jobParams.datasets.toString}")
    val successfulDatasets = new ConcurrentLinkedQueue[String]()

    // Getting the datasets that were correctly processed in the previous Autoloader task of the workflow job.
    val previousTask = workflowTaskExecutionDao.getWorkflowTaskExecutionByJobRunId(jobRunID)
      .filter(_.task_name == IngestionTaskTags.taskKey)

    val filteredDatasets= if (previousTask.isEmpty) {
      logger.info(s"No previous task (Autoloader) were found. All input datasets will be processed.")
      jobParams.datasets
    } else {
      logger.info(s"A previous task (Autoloader) was found. Input datasets will be filtered according to the datasets correctly processed in this previous task.")
      val successfulIngestionDatasets =
        previousTask
          .filter(_.status == "Success")
          .maxBy(_.id)
          .successful_datasets
          .trim
          .split(",")
          .filter(_.nonEmpty)

      logger.info(s"The following datasets were correctly processed in the previous Autoloader task of the workflow job: " +
        s"${successfulIngestionDatasets.mkString(",")}")

      jobParams.datasets.filter(dataset => successfulIngestionDatasets.contains(dataset.dataset))
    }

    if (filteredDatasets.isEmpty) {
      logger.info(s"No datasets needs to be processed")
      List[String]()
    } else {
      filteredDatasets.par.foreach{ds =>
        val datasetStartTime = System.currentTimeMillis()
        logger.info(s"The quality process starts for ${ds.dataset} dataset")
        val datasetExecutionInit = WorkflowDatasetExecution(
          dataset = ds.dataset,
          source = jobParams.source,
          task_name = taskKey,
          task_run_id = taskRunId,
          dataset_process_utc_start_time = new Timestamp(datasetStartTime)
        )

        val datasetProcess = Try(runQualityDataset(jobParams.source, ds))
        val datasetEndTime = System.currentTimeMillis()
        val datasetDuration = (datasetEndTime - datasetStartTime) / 1000

        val datasetStatus = datasetProcess match {
          case Success(Right(rowCounts)) =>
            logger.info(s"Quality worked fine for dataset ${ds.dataset}.\n")
            successfulDatasets.add(ds.dataset)

            datasetExecutionInit.copy(
              dataset_process_duration = datasetDuration,
              dataset_process_utc_end_time = new Timestamp(datasetEndTime),
              status = "Success",
              read_rows = rowCounts._1,
              written_rows = rowCounts._2
            )

          case Success(Left(exception)) =>
            logger.info(s"Quality failed unexpectedly for dataset ${ds.dataset}.\n" +
              s"Reason: ${exception.getMessage}")

            datasetExecutionInit.copy(
              dataset_process_duration = datasetDuration,
              dataset_process_utc_end_time = new Timestamp(datasetEndTime),
              status = "Failure",
              error_message = exception.getMessage.split("Stack trace:")(0).trim
            )

          case Failure(exception) =>
            datasetExecutionDao.insertInto(
              datasetExecutionInit.copy(
                dataset_process_duration = datasetDuration,
                dataset_process_utc_end_time = new Timestamp(datasetEndTime),
                status = "Failure",
                error_message = exception.getMessage.split("Stack trace:")(0).trim
              )
            )
            throw new QualityManagerException(
              s"Quality failed unexpectedly for dataset ${ds.dataset}.\n" +
              s"Reason: ${exception.getMessage}\n"
            )
        }
        datasetExecutionDao.insertInto(datasetStatus)
      }
      successfulDatasets.asScala.toList
    }
  }

  /**
   * Ejecuta la calidad de un dataset: lee de raw_sources según su tipo de extracción,
   * aplica sus reglas de calidad, escribe en raw_clean y actualiza la última versión
   * Delta procesada del flujo raw_sources → raw_clean.
   *
   * @param source source del dataset
   * @param ds     dataset a procesar
   * @return Right con (filas leídas, filas escritas), o Left con el error
   */
  private def runQualityDataset(source: String, ds: Dataset): Either[QualityManagerException, (Long, Long)] = {
    for {
      // Reading
      dfRawSource <- Try(readDataFromRawSource(source, ds.dataset, ds.extract_type)) match {
        case Success(Right(df)) => Right(df)
        case Success(Left(e)) => Left(new QualityManagerException(
          s"Quality reading error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e))
        case Failure(e) => throw new QualityManagerException(
          s"Quality reading configuration error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e)
      }

      // Obtaining and applying quality rules
      dfRawSourceCleaned <- Try {
        val qualityRules = qualityDao.getRulesById(ds.id)
        applyQualityRules(dfRawSource, qualityRules)
      } match {
        case Success(df) => Right(df)
        case Failure(e) => Left(new QualityManagerException(
          s"Error obtaining and applying quality rules: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e))
      }

      // Writing
      _ <- Try(writeDfToRawClean(dfRawSourceCleaned, source, ds)) match {
        case Success(Right(_)) =>
          val readDeltaVersion = DeltaChangeDataFeed.getLatestSignificantDeltaVersion(CommonsConstants.RAW_SOURCES, source, ds.dataset)
          latestProcessedVersionsDao.insertIntoOnConflict(
            LatestProcessedVersions(
              source=source,
              read_layer=CommonsConstants.RAW_SOURCES,
              read_table=ds.dataset,
              last_read_processed_version=readDeltaVersion,
              target_layer=CommonsConstants.RAW_CLEAN,
              target_table=ds.dataset,
              task_run_id=taskRunId
            )
          )
          Right(())
        case Success(Left(e)) => Left(new QualityManagerException(
          s"Quality writing error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e))
        case Failure(e) => throw new QualityManagerException(
          s"Quality writing configuration error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e)
      }

    } yield (dfRawSource.count(), dfRawSourceCleaned.count())
  }

  /**
   * Aplica en orden las reglas de calidad al DataFrame; si no hay reglas lo
   * devuelve tal cual.
   *
   * @param dfRawSource  DataFrame leído de raw_sources
   * @param qualityRules reglas de calidad del dataset
   * @return el DataFrame con las reglas aplicadas
   */
  def applyQualityRules(dfRawSource: DataFrame, qualityRules: Seq[Quality]): DataFrame = {
    if (qualityRules.nonEmpty) {
      qualityRules.foldLeft(dfRawSource) { (dfRS, qualityRule) =>
        logger.info(s"Start quality rule: ${qualityRule.name} to be executed for fields ${qualityRule.fields}")
        executeTypeQuality(dfRS, qualityRule.rule, qualityRule.fields, qualityRule.value, qualityRule.replacement)
      }
    } else {
      logger.info("Dataset without Quality Rules")
      dfRawSource
    }
  }

  /**
   * Reads data from the raw/source layer and returns a DataFrame
   * Escenarios:
   *    - Extracción FULL => Lectura norma de una tabla delta - readDeltaTable
   *    - Extracción DELTA (Incremental, solo insert) => Lectura de la última versión de tabla delta con CDF y filtrado
   *    de registros con operación 'insert'
   *    - Extracción CDC (insert, update & delete) => Lectura de la última versión de una tabla delta con CDF
   *
   * @param source      The name of the data source in the raw layer
   * @param datasetName The name of the dataset in the raw layer
   * @param extractType The type of extraction to perform: FULL, DELTA or CDC
   * @return A DataFrame representing the requested data
   * @throws IllegalArgumentException if the extraction type is invalid or if an error occurs while reading the data
   */
  private def readDataFromRawSource(source: String, datasetName: String, extractType: String): Either[Throwable, DataFrame] = {
    logger.info(s"Start method to read data from raw_sources layer: $source and $datasetName: Mode: $extractType")

    val readDeltaVersion = DeltaChangeDataFeed.getLatestSignificantDeltaVersion(CommonsConstants.RAW_SOURCES, source, datasetName)
    val lastRegisteredVersion = latestProcessedVersionsDao
      .getLatestProcessedVersionsByUniqueKey(source, CommonsConstants.RAW_SOURCES, datasetName, CommonsConstants.RAW_CLEAN, datasetName)
      .headOption
      .map(_.last_read_processed_version)
      .getOrElse(-1L)

    val checkDeltaVersion = Set(
      ConstantsQuality.FULL_PARTITION_EXTRACTION,
      ConstantsQuality.DELTA_EXTRACTION,
      ConstantsQuality.STREAM_EXTRACTION,
      ConstantsQuality.CDC_EXTRACTION
    ).contains(extractType.toUpperCase)

    if (checkDeltaVersion && readDeltaVersion <= lastRegisteredVersion) {
      logger.info(s"It's not possible to process a read version equal to or less than the last registered" +
        s" for ${CommonsConstants.RAW_SOURCES}.$source.$datasetName table" +
        s" (Read version: $readDeltaVersion, last processed version: $lastRegisteredVersion)")
      return Left(new QualityManagerException(s"It's not possible to process a read version equal to or less than the last registered" +
          s" for ${CommonsConstants.RAW_SOURCES}.$source.$datasetName table" +
          s" (Read version: $readDeltaVersion, last processed version: $lastRegisteredVersion)"
      ))
    }

    extractType.toUpperCase match {
      case ConstantsQuality.FULL_PARTITION_EXTRACTION | ConstantsQuality.DELTA_EXTRACTION | ConstantsQuality.STREAM_EXTRACTION =>
        DeltaChangeDataFeed.readDeltaFeedFromVersion(CommonsConstants.RAW_SOURCES, source, datasetName, readDeltaVersion)
      case ConstantsQuality.CDC_EXTRACTION =>
        DeltaChangeDataFeed.readDeltaFeedFromVersionCDC(CommonsConstants.RAW_SOURCES, source, datasetName, readDeltaVersion)
      case ConstantsQuality.FULL_EXTRACTION =>
        DeltaTablesManager.readDeltaTable(CommonsConstants.RAW_SOURCES, source, datasetName)
      case _ =>
        throw new IllegalArgumentException(s"Invalid extract type: $extractType")
    }
  }

  /**
   * Executes a quality rule action on a DataFrame
   *
   * @param df DataFrame to apply the quality rule to
   * @param qualityRule String with the type of quality rule action to execute
   * @param fields String with the name of the field(s) to apply the quality rule to
   * @param value String with the value to use for the quality rule action (if applicable)
   * @param replacement String with the replacement value to use for some quality rule actions (if applicable)
   * @return DataFrame resulting from the quality rule action
   * @throws Exception if the quality rule action fails
   * @throws QualityManagerException if the quality rule is not one of the supported ones
   */
  private def executeTypeQuality(df: DataFrame, qualityRule: String, fields: String, value: String, replacement: String): DataFrame = {
    logger.info(s"Run Quality method: $qualityRule")

    val resultRuleQuality = qualityRule match {
      case ConstantsQuality.FILTER_EQUAL_PARAM => qualityDefault.filterEqualDataFrame(df, fields, value)
      case ConstantsQuality.FILTER_NOTEQUAL_PARAM => qualityDefault.filterNotEqualDataFrame(df, fields, value)
      case ConstantsQuality.FILTER_REGEX_PARAM => qualityDefault.filterRegexDataFrame(df, fields, value)
      case ConstantsQuality.FILTER_RANGE_PARAM => qualityDefault.filterRangeDataFrame(df, fields, value)
      case ConstantsQuality.FILTER_NOT_NULL_PARAM => qualityDefault.filterNotNullDataFrame(df, fields, value)
      case ConstantsQuality.REPLACE_PARAM => qualityDefault.replaceColumnWhen(df, fields, value)
      case ConstantsQuality.REPLACE_NULL_PARAM => qualityDefault.fillNulls(df, fields, value)
      case ConstantsQuality.DROP_PARAM => qualityDefault.dropColumns(df, fields)
      case ConstantsQuality.DROP_DUPLICATE_PARAM => qualityDefault.dropDuplicates(df, fields)
      case ConstantsQuality.REGEXP_PARAM => qualityDefault.regexp(df, fields, value, replacement)
      case ConstantsQuality.FORMATDATES_PARAM => qualityDefault.formatDates(df, fields, value)
      case ConstantsQuality.SAP_DATE_PARAM => qualitySAP.sapDateFormat(df, fields)
      case ConstantsQuality.FLATTEN_JSON_PARAM => qualityDefault.flattenJsonField(df, fields)
      case ConstantsQuality.FLATTEN_STRUCT_PARAM => qualityDefault.flattenStructField(df, fields)
      case ConstantsQuality.JSON_TO_COMPLEX_TYPES_PARAM => qualityJson.jsonToComplexType(df)
      case ConstantsQuality.ADD_YEAR_MONTH_DAY_COLUMNS_PARAM => qualityDefault.addDateColumns(df, fields)
      case ConstantsQuality.FORMAT_DATE_STRING => qualityDefault.formatDatesAsString(df, fields, value)
      case ConstantsQuality.FORMAT_DATE_STRING_NEW => qualityDefault.formatDatesAsStringNew(df, fields, value)
      case ConstantsQuality.EXPLODE_PARAM => qualityDefault.explodeDelimitedColumn(df, fields, value)
      case ConstantsQuality.UNIXTOTIMESTAMP_SECOND => qualityDefault.unixToTimestamp(df, fields, "second")
      case ConstantsQuality.UNIXTOTIMESTAMP_MILLISECOND => qualityDefault.unixToTimestamp(df, fields, "millisecond")
      case ConstantsQuality.UNIXTOTIMESTAMP_MICROSECOND => qualityDefault.unixToTimestamp(df, fields, "microsecond")
      case _ =>
        logger.error(s"Unsupported quality rule: $qualityRule (fields: $fields). " +
          s"Check the 'rule' column of the metadata.quality table")
        throw new QualityManagerException(s"Unsupported quality rule: $qualityRule (fields: $fields). " +
          s"Check the 'rule' column of the metadata.quality table")
    }
    resultRuleQuality match {
      case Right(dataFrame) => dataFrame
      case Left(exception) => throw new Exception(s"Error running rule: $qualityRule\n${exception.getMessage}", exception)
    }
  }


  /**
   * Writes a DataFrame to the "raw clean" layer, using the path and options specific to the provided dataset.
   * Depending on the dataset's extraction type, the data may be overwritten, appended, or merged with existing Delta data.
   * If necessary, the method creates the schema in the "raw clean" layer before writing the DataFrame.
   *
   * Escenarios:
   *    - Extracción FULL => escritura normal Overwrite - sparkWriteData | 'saveAsTable'
   *    - Extracción DELTA (Incremental, solo insert) => escritura normal Append - sparkWriteData | 'saveAsTable'
   *    - Extracción CDC (insert, update & delete) =>
   *              * Si no existe la tabla delta => Escritura normal Append - sparkWriteData | 'saveAsTable'
   *              * Si existe la tabla delta => Custom upsert Change Data Capture
   *
   * @param df the DataFrame to be written
   * @param source the source system or process associated with the DataFrame
   * @param dataset the dataset representing the metadata of the DataFrame
   * @throws QualityDefaultException in case of any error
   */
  def writeDfToRawClean(df: DataFrame, source: String, dataset: Dataset): Either[Throwable, Unit] = {
    logger.info(s"Write DataFrame cleaned to 'raw clean' layer: ${dataset.dataset}")

    val dsRawCleanPath = ConstructorPaths.buildRawCleanPath(s"$source", dataset.dataset)
    // Retorna una lista vacía en caso de que partition_by sea NULL en la BD
    val partitions = Option(dataset.delta_column)
      .map(_.split(",").map(_.trim).toList).getOrElse(List.empty[String])

    createDatabase(s"${CommonsConstants.RAW_CLEAN}.$source") match {
      case Right(schemaName) =>
        dataset.extract_type match {
          case ConstantsQuality.FULL_PARTITION_EXTRACTION =>
            logger.info(s"Write DataFrame cleaned to 'raw clean' layer: ${dataset.dataset} in FULL_PARTITION Mode")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite,
              partitionBy = partitions, otherOptions = Map("partitionOverwriteMode"-> "dynamic"))
            sparkWriteData(s"${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", df, dsRawCleanPath, "saveAsTable", sparkOptions)

          case ConstantsQuality.FULL_EXTRACTION =>
            logger.info(s"Write DataFrame cleaned to 'raw clean' layer: ${dataset.dataset} in FULL Mode")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite)
            sparkWriteData(s"${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", df, dsRawCleanPath, "saveAsTable", sparkOptions)

          case ConstantsQuality.DELTA_EXTRACTION | ConstantsQuality.STREAM_EXTRACTION =>
            logger.info(s"Write DataFrame cleaned to 'raw clean' layer: ${dataset.dataset} in INCREMENTAL (DELTA) Mode")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Append,
              partitionBy = partitions)
            sparkWriteData(s"${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", df, dsRawCleanPath, "saveAsTable", sparkOptions)

          case ConstantsQuality.CDC_EXTRACTION =>
            logger.info(s"Write DataFrame cleaned to 'raw clean' layer: ${dataset.dataset} in CDC Mode")

            if(DeltaTable.isDeltaTable(SparkConfiguration.spark, dsRawCleanPath)) {
              logger.info(s"The Delta Table exists: Execute Merge Delta Table with Quality module in $dsRawCleanPath")
              mergeDeltaTableQuality(dsRawCleanPath, df, dataset)

            } else {
              logger.info(s"The Delta Table not exists: Execute Spark Write Data 'saveAsTable' with Quality module in $dsRawCleanPath")
              val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Append,
                partitionBy = partitions)
              sparkWriteData(s"${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", df, dsRawCleanPath, "saveAsTable", sparkOptions)
            }
        }
      case Left(exception) =>
        throw new QualityDefaultException(s"Error creating schema: ${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", exception)
    }
  }


  /**
   * Method to merge delta table in Quality layer
   *
   * @param datasetAdlsPath
   * @param df
   * @param dataset
   * @return
   */
  private def mergeDeltaTableQuality(datasetAdlsPath: String, df: DataFrame, dataset: Dataset): Either[Throwable, Unit] = {
    logger.info(s"Start merge Delta table in Quality layer in $datasetAdlsPath")
    val targetDeltaTable = DeltaTable.forPath(SparkConfiguration.spark, datasetAdlsPath)

    upsertDeltaTableChangeDataCapture(targetDeltaTable, df, dataset.key_columns.split(","))
  }


  /**
   * Marca el fin del job de calidad (solo log).
   *
   * @return mensaje de fin del job
   */
  def finishTasksQuality(): String = {
    logger.info("End Quality Databricks Job")
    val message = """
          ####    ####   ####         ####   ############     ########    ############   ####     ########
          ####    ####     ####      ####    ####    ####   ####    ####      ####       ####   ####    ####
          ############       ####  ####      ####    ####   ####    ####      ####              ####    ####
          ############          ####         ############   ############      ####       ####   ############
          ####    ####          ####         ####           ####    ####      ####       ####   ####    ####
          ####    ####          ####         ####           ####    ####      ####       ####   ####    ####
          ####    ####          ####         ####           ####    ####      ####       ####   ####    ####

          ############      ####    ####  ############  ####      ####  ############  ####    ####
          ####    ####      ####    ####  ####    ####  ####      ####      ####      ####    ####
          ####    ####      ####    ####  ############  ####      ####      ####      ############
          ################  ############  ####    ####  ########  ####      ####           ###

          ########  ####  ########  ####  ####  ############  ####    ####  ####
          ####            ####  ##  ####        ########      ####    ####  ####
          ########  ####  ####  ##  ####  ####  ############  ############  ####
          ####      ####  ####  ##  ####  ####         #####  ############  ####
          ####      ####  ####  ##  ####  ####  ############  ####    ####
          ####      ####  ####  ########  ####  ############  ####    ####  ####
        """
    println(message)

    "End Quality Databricks Job"
  }
}

