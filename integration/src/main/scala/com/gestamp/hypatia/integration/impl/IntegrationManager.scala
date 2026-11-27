package com.gestamp.hypatia.integration.impl

import com.gestamp.hypatia.adls.ADLS2Manager.setADLS2OAuthSparkConfiguration
import com.gestamp.hypatia.adls.ADLS2Manager.sparkWriteData
import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.databricks.deltalake.DeltaChangeDataFeed.upsertDeltaTableChangeDataCapture
import com.gestamp.hypatia.databricks.deltalake.{DeltaChangeDataFeed, DeltaTablesManager}
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.createDatabase
import com.gestamp.hypatia.integration.args.IntegrationJobParams
import com.gestamp.hypatia.integration.exception.IntegrationManagerException
import com.gestamp.hypatia.integration.logic.IntegrationOperator
import com.gestamp.hypatia.integration.utils.{Config, ConstantsIntegration}
import com.gestamp.hypatia.postgresql.dao._
import com.gestamp.hypatia.postgresql.model._
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import com.gestamp.hypatia.utils.CommonsConstants
import com.gestamp.hypatia.utils.JsonTools.jsonToCaseClass
import com.gestamp.hypatia.integration.utils.SchemaCsvFileReader.castDataFrameByCSVSchema
import com.gestamp.hypatia.postgresql.dao.{WorkflowDatasetExecutionDao, WorkflowTaskExecutionDao}
import com.gestamp.hypatia.utils.WorkflowTags.jobRunID
import com.gestamp.hypatia.utils.IntegrationTaskTags.{taskKey, taskRunId}
import com.gestamp.hypatia.utils.QualityTaskTags
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentLinkedQueue
import scala.util.{Failure, Left, Right, Success, Try}

object IntegrationManager {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val distillationDao: DistillationDao = new DistillationDao
  private val datasetExecutionDao: WorkflowDatasetExecutionDao = new WorkflowDatasetExecutionDao
  private val workflowTaskExecutionDao: WorkflowTaskExecutionDao = new WorkflowTaskExecutionDao


  /**
   * Orquesta el proceso de integración: filtra las distillations a las de datasets
   * que salieron bien en la task previa de Quality del mismo job (si existe),
   * procesa cada una en paralelo (lectura de raw_clean con o sin taxonomía,
   * aplicación del esquema CSV y escritura en integration) y registra la ejecución
   * de cada dataset en orchestration.
   *
   * @param jobParams parámetros del job (source y distillations ya resueltas)
   * @return los nombres de las distillations procesadas correctamente
   */
  def handleData(jobParams: IntegrationJobParams): List[String] = {
    logger.info(s"Data handling is started in the integration process. Datasets: ${jobParams.distillations.map(d => s""""${d.dataset_name}"""").mkString(", ")}")
    val successfulDatasets = new ConcurrentLinkedQueue[String]()

    // Getting the datasets that were correctly processed in the previous quality task of the workflow job.
    val previousTask = workflowTaskExecutionDao.getWorkflowTaskExecutionByJobRunId(jobRunID)
      .filter(_.task_name == QualityTaskTags.taskKey)

    val filteredDistillations = if (previousTask.isEmpty) {
      logger.info(s"No previous task (quality) were found. All input datasets will be processed.")
      jobParams.distillations
    } else {
      logger.info(s"A previous task (quality) was found. Input datasets will be filtered according to the datasets correctly processed in this previous task.")
      val successfulQualityDatasets =
        previousTask
          .filter(_.status == "Success")
          .maxBy(_.id)
          .successful_datasets
          .trim
          .split(",")
          .filter(_.nonEmpty)

      logger.info(s"The following datasets were correctly processed in the previous Quality task of the workflow job: " +
        s"${successfulQualityDatasets.mkString(",")}")

      jobParams.distillations.filter(datarelationship => successfulQualityDatasets.contains(datarelationship.dataset_name))
    }

    setADLS2OAuthSparkConfiguration()

    if (filteredDistillations.isEmpty) {
      logger.info(s"No distillations needs to be processed")
      List[String]()
    } else {
      val distillationsToProcess: Seq[Int] = filteredDistillations.map(datarelationship => datarelationship.distillation_id).distinct
      val distillations: Seq[Distillation] = distillationDao.getDistillationBySequenceIds(distillationsToProcess)
      logger.info(s"Distillations to be processed in the current integration task: ${distillations.map(_.datum).mkString(",")}")

      distillations.par.foreach { dis =>
        val datasetStartTime = System.currentTimeMillis()
        logger.info(s"The integration process starts for ${dis.datum} dataset")
        val datasetExecutionInit = WorkflowDatasetExecution(
          dataset = dis.datum,
          source = jobParams.source,
          task_name = taskKey,
          task_run_id = taskRunId,
          dataset_process_utc_start_time = new Timestamp(datasetStartTime)
        )

        val distRelationship = jobParams.distillations.filter(_.distillation_id == dis.id)

        val datasetProcess = Try(runIntegrationDataset(jobParams.source, dis, distRelationship))
        val datasetEndTime = System.currentTimeMillis()
        val datasetDuration = (datasetEndTime - datasetStartTime) / 1000

        val datasetStatus = datasetProcess match {
          case Success(Right(rowCounts)) =>
            logger.info(s"Integration worked fine for distillation dataset ${dis.datum}.\n")
            successfulDatasets.add(dis.datum)


            datasetExecutionInit.copy(
              dataset_process_duration = datasetDuration,
              dataset_process_utc_end_time = new Timestamp(datasetEndTime),
              status = "Success",
              read_rows = rowCounts,
              written_rows = rowCounts
            )


          case Success(Left(exception)) =>
            logger.info(s"Integration failed unexpectedly for distillation dataset ${dis.datum}.\n" +
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
            logger.info(s"Integration failed unexpectedly for distillation dataset ${dis.datum}. The task execution will be marked as failed.\n" +
              s"Reason: ${exception.getMessage}")

            throw new IntegrationManagerException(
              s"Integration failed unexpectedly for distillation dataset ${dis.datum}.\n" +
              s"Reason: ${exception.getMessage}\n"
            )
        }
        datasetExecutionDao.insertInto(datasetStatus)
      }
      successfulDatasets.asScala.toList
    }
  }

  /**
   * Ejecuta la integración de una distillation: lee de raw_clean (aplicando la
   * taxonomía si la relación define operación), recastea con el esquema CSV y
   * escribe en la capa integration.
   *
   * @param source           source de los datos
   * @param distillation     distillation destino
   * @param dataRelationship relaciones dataset → distillation que la definen
   * @return Right con las filas leídas, o Left con el error
   */
  private def runIntegrationDataset(source: String, distillation: Distillation, dataRelationship: Seq[DataRelationship]): Either[IntegrationManagerException, Long] = {
    for {
      // Reading
      dfRawClean <- Try {
        val operation = dataRelationship.head.operation
        if (operation.nonEmpty)
          readDataFromRawCleanWithOperation(source, distillation, operation)
        else {
          val rawName = dataRelationship.head.dataset_name
          readDataFromRawClean(source, rawName, distillation.ingest_type)
        }
      } match {
        case Success(Right(df)) => Right(df)
        case Success(Left(e)) =>
          Left(new IntegrationManagerException(
            if (!e.getMessage.contains("Stack trace:"))
              s"Integration reading error: ${e.getMessage}\n" +
              s"Stack trace: ${e.getStackTrace.mkString("\n")}"
            else
              s"Integration reading error: ${e.getMessage}",
            e
          ))
        case Failure(e) => throw new IntegrationManagerException(
          if (!e.getMessage.contains("Stack trace:"))
            s"Integration reading configuration error: ${e.getMessage}\n" +
              s"Stack trace: ${e.getStackTrace.mkString("\n")}"
          else
            s"Integration reading configuration error: ${e.getMessage}: ${e.getMessage}",
          e
        )
      }

      // Applying CSV schema
      dfRawCleanNewSchema <- Try {
        val dfNewSchema = castDataFrameByCSVSchema(source, distillation.datum, dfRawClean)
        dfNewSchema
      } match {
        case Success(df) => Right(df)
        case Failure(e) => throw new IntegrationManagerException(
          s"Error applying CSV schema (configuration error): ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e)
      }

      // Writing
      _ <- Try(writeDfToIntegration(dfRawCleanNewSchema, source, distillation)) match {
        case Success(Right(_)) => Right(())
        case Success(Left(e)) => Left(new IntegrationManagerException(
          s"Integration writing error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e))
        case Failure(e) => throw new IntegrationManagerException(
          s"Integration writing configuration error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e)
      }
    } yield dfRawClean.count()
  }


  /**
   * Lee de raw_clean aplicando la taxonomía definida en el JSON de operación
   * (union/join de varios datasets) a través del IntegrationOperator.
   *
   * @param source       source de los datos
   * @param distillation distillation destino
   * @param operation    JSON de la taxonomía a aplicar
   * @return Right con el DataFrame resultante, o Left con el error
   */
  private def readDataFromRawCleanWithOperation(source: String, distillation: Distillation, operation: String): Either[Throwable, DataFrame] = {
    logger.info(s"JSON taxonomy: $operation")
    Try{
      val config = jsonToCaseClass[Config](operation)
      val taxonomyDf = IntegrationOperator.applyOperation(config, source, distillation)
      taxonomyDf
    } match {
      case Success(df) => Right(df)
      case Failure(e) => throw new IntegrationManagerException(
        s"Taxonomy configuration error: ${e.getMessage}\n",
        e)
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
  private def readDataFromRawClean(source: String, datasetName: String, extractType: String): Either[Throwable, DataFrame] = {
    logger.info(s"Start method to read data from raw_clean layer: $source and $datasetName: Mode: $extractType")

    val readDeltaVersion = DeltaChangeDataFeed.getLatestSignificantDeltaVersion(CommonsConstants.RAW_CLEAN, source, datasetName)

    extractType.toUpperCase match {
      case ConstantsIntegration.FULL_PARTITION_EXTRACTION | ConstantsIntegration.INCREMENTAL_EXTRACTION | ConstantsIntegration.STREAM_EXTRACTION =>
        DeltaChangeDataFeed.readDeltaFeedFromVersion(CommonsConstants.RAW_CLEAN, source, datasetName, readDeltaVersion)
      case ConstantsIntegration.CDC_EXTRACTION =>
        DeltaChangeDataFeed.readDeltaFeedFromVersionCDC(CommonsConstants.RAW_CLEAN, source, datasetName, readDeltaVersion)
      case ConstantsIntegration.FULL_EXTRACTION =>
        DeltaTablesManager.readDeltaTable(CommonsConstants.RAW_CLEAN, source, datasetName)
      case _ =>
        throw new IllegalArgumentException("Invalid extract type")
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
   * @param dfRawCleanNewSchema the DataFrame to be written
   * @param source the source system or process associated with the DataFrame
   * @param dataset the dataset representing the metadata of the DataFrame
   * @throws QualityDefaultException in case of any error
   */
  //TODO: distillation.ingest_type por dataset.extract_type
  //TODO: cambiar nombre extract_type por flow_mode
  def writeDfToIntegration(dfRawCleanNewSchema: DataFrame, source: String, dataset: Distillation): Either[Throwable, Unit] = {
    logger.info(s"Write DataFrame cleaned to 'Integration' layer: ${dataset.datum}")

    val dsIntegrationPath = ConstructorPaths.buildSilverIntegrationPath(s"$source", dataset.datum)
    // Retorna una lista vacía en caso de que partition_by sea NULL en la BD
    val partitions = Option(dataset.partition_by)
      .map(_.split(",").map(_.trim).toList).getOrElse(List.empty[String])

    createDatabase(s"${CommonsConstants.INTEGRATION}.$source") match {
      case Right(schemaName) =>
        dataset.ingest_type match {
          case ConstantsIntegration.FULL_PARTITION_EXTRACTION =>
            logger.info(s"Write DataFrame cleaned to 'raw clean' layer: ${dataset.datum} in FULL_PARTITION Mode")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite,
              partitionBy = partitions, otherOptions = Map("partitionOverwriteMode"-> "dynamic"))
            sparkWriteData(s"${CommonsConstants.INTEGRATION}.$source.${dataset.datum}", dfRawCleanNewSchema, dsIntegrationPath, "saveAsTable", sparkOptions)

          case ConstantsIntegration.FULL_EXTRACTION =>
            logger.info(s"Write DataFrame cleaned to 'Integration' layer: ${dataset.datum} in FULL Mode")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite,
              partitionBy = partitions)
            sparkWriteData(s"${CommonsConstants.INTEGRATION}.$source.${dataset.datum}", dfRawCleanNewSchema, dsIntegrationPath, "saveAsTable", sparkOptions)

          case ConstantsIntegration.INCREMENTAL_EXTRACTION =>
            //TODO: Recarga puntual de datos
            logger.info(s"Write DataFrame cleaned to 'Integration' layer: ${dataset.datum} in INCREMENTAL (DELTA) Mode")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Append,
              partitionBy = partitions)
            sparkWriteData(s"${CommonsConstants.INTEGRATION}.$source.${dataset.datum}", dfRawCleanNewSchema, dsIntegrationPath, "saveAsTable", sparkOptions)

          case ConstantsIntegration.CDC_EXTRACTION =>
            logger.info(s"Write DataFrame cleaned to 'Integration' layer: ${dataset.datum} in CDC Mode")

            if(DeltaTable.isDeltaTable(SparkConfiguration.spark, dsIntegrationPath)) {
              logger.info(s"The Delta Table exists: Execute Merge Delta Table with Quality module in $dsIntegrationPath")
              mergeDeltaTableIntegration(dsIntegrationPath, dfRawCleanNewSchema, dataset)

            } else {
              logger.info(s"The Delta Table not exists: Execute Spark Write Data 'saveAsTable' with Quality module in $dsIntegrationPath")
              val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Append,
                partitionBy = partitions)
              sparkWriteData(s"${CommonsConstants.INTEGRATION}.$source.${dataset.datum}", dfRawCleanNewSchema, dsIntegrationPath, "saveAsTable", sparkOptions)
            }
        }
      case Left(exception) =>
        throw new IntegrationManagerException(s"Error creating schema: ${CommonsConstants.INTEGRATION}.$source.${dataset.datum}", exception)
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
  //TODO: ¿Cómo particionas una tabla delta en modo CDC?
  private def mergeDeltaTableIntegration(datasetAdlsPath: String, df: DataFrame, dataset: Distillation): Either[Throwable, Unit] = {
    logger.info(s"Start merge Delta table in Quality layer in $datasetAdlsPath")
    val targetDeltaTable = DeltaTable.forPath(SparkConfiguration.spark, datasetAdlsPath)

    upsertDeltaTableChangeDataCapture(targetDeltaTable, df, dataset.key_columns.split(","))
  }

  /**
   * Marca el fin del job de integración (solo log).
   *
   * @return mensaje de fin del job
   */
  def finishTasksIntegration(): String = {
    logger.info("End Integration Databricks Job")
    "End Integration Databricks Job"
  }

}
