package com.gestamp.hypatia.impl

import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.args.IngestionJobParams
import com.gestamp.hypatia.autoloader.AutoLoader
import com.gestamp.hypatia.config.AutoLoaderConfigBuilder
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.createDatabase
import com.gestamp.hypatia.exceptions.{AutoLoaderException, IngestionManagerException}
import com.gestamp.hypatia.postgresql.dao.WorkflowDatasetExecutionDao
import com.gestamp.hypatia.postgresql.model.{Dataset, WorkflowDatasetExecution}
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import com.gestamp.hypatia.utils.{CommonsConstants, IngestionConstants}
import io.delta.tables.DeltaTable
import com.gestamp.hypatia.utils.IngestionTaskTags.{taskKey, taskRunId}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentLinkedQueue
import scala.util.{Failure, Success, Try}

object IngestionManager {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val datasetExecutionDao: WorkflowDatasetExecutionDao = new WorkflowDatasetExecutionDao

  /**
   * Orchestrator method of ingestion of files and Delta objects. This method will go through datasets entered
   * by parameter and will show the Delta Objects of the datasets ingested in the raw ADLS layer and raw_source catalog.
   *
   * @throws IngestionManagerException if an error occurs processing a dataset
   */
  def handleData(jobParams: IngestionJobParams): List[String] = {
    logger.info(s"Data handling is started in the ingestion process. Datasets: ${jobParams.datasets.toString}")
    val successfulDatasets = new ConcurrentLinkedQueue[String]()

    jobParams.datasets.par.foreach{ds => {
      val datasetStartTime = System.currentTimeMillis()
      logger.info(s"The ingestion process starts for ${ds.dataset} dataset")

      val datasetExecutionInit = WorkflowDatasetExecution(
        dataset = ds.dataset,
        source = jobParams.source,
        task_name = taskKey,
        task_run_id = taskRunId,
        dataset_process_utc_start_time = new Timestamp(datasetStartTime)
      )

      val datasetProcess = Try(runIngestionDataset(jobParams.source, ds))
      val datasetEndTime = System.currentTimeMillis()
      val datasetDuration = (datasetEndTime - datasetStartTime) / 1000

      val datasetStatus = datasetProcess match {
        case Success(Right(rowCounts)) =>
          logger.info(s"Autoloader worked fine for dataset ${ds.dataset}.\n")
          successfulDatasets.add(ds.dataset)

          datasetExecutionInit.copy(
            dataset_process_duration = datasetDuration,
            dataset_process_utc_end_time = new Timestamp(datasetEndTime),
            status = "Success",
            read_rows = rowCounts._1,
            written_rows = rowCounts._2
          )

        case Success(Left(exception)) =>
          logger.info(s"Autoloader failed unexpectedly for dataset ${ds.dataset}.\n" +
            s"Reason: ${exception.getMessage}\n")

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
          throw new IngestionManagerException(
            s"Autoloader failed unexpectedly for dataset ${ds.dataset}.\n" +
            s"Reason: ${exception.getMessage}\n"
          )
      }
      datasetExecutionDao.insertInto(datasetStatus)
    }
    }
    successfulDatasets.asScala.toList
  }

  /**
   * Ejecuta la ingesta de un dataset: lee los ficheros nuevos de landing con el
   * Auto Loader y los escribe en raw_sources, encadenando lectura y escritura.
   *
   * @param source source del dataset
   * @param ds     dataset a ingestar
   * @return Right con (filas leídas, filas escritas), o Left con el error
   */
  private def runIngestionDataset(source: String, ds: Dataset): Either[IngestionManagerException, (Long, Long)] = {
    for {
      // Reading
      landingDf <- Try(readNewFiles(source, ds)) match {
        case Success(Right(df)) => Right(df)
        case Success(Left(e)) => Left(new IngestionManagerException(
          s"Autoloader reader error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e))
        case Failure(e) => throw new IngestionManagerException(
          s"Autoloader reader configuration error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e)
      }
      // Writing
      counts <- Try(writeDfToRawSource(landingDf, source, ds)) match {
        case Success(Right(counts)) => Right(counts)
        case Success(Left(e)) => Left(new IngestionManagerException(
          s"Autoloader writer error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e))
        case Failure(e) => throw new IngestionManagerException(
          s"Autoloader writer configuration error: ${e.getMessage}\n" +
          s"Stack trace: ${e.getStackTrace.mkString("\n")}",
          e)
      }

    } yield counts
  }


  /**
   * Method to call Auto Loader Read Stream and read new files in landing layer
   *
   * @throws AutoLoaderException if an error occurs reading data
   */
  def readNewFiles(source: String, dataset: Dataset): Either[Throwable, DataFrame] = {
    logger.info(s"Read dataset ${dataset.dataset} from landing layer")

    val autoloaderConfig = new AutoLoaderConfigBuilder(dataset, source).getAutoLoaderConfig()
    val autoloaderConfigMergeSchema = if (List("parquet", "avro").contains(dataset.format)) {
      autoloaderConfig + ("mergeSchema" -> "true")
    } else autoloaderConfig
    AutoLoader.sparkReadStream(autoloaderConfigMergeSchema)
  }


  /**
   * Method to call Auto Loader Write Stream and write new files in raw/source layer in Delta format and generate
   * new Delta Object in our LakeHouse
   * - Depends on type of extraction we have to append data or overwrite new data
   *
   * @throws IngestionManagerException if an error occurs writing data to Delta
   */
  //TODO: dataset.delta_column por dataset.partition_by
  private def writeDfToRawSource(dataFrame: DataFrame, source: String, dataset: Dataset): Either[Throwable,(Long,Long)] = {
    logger.info(s"Write new files for dataset ${dataset.dataset} to 'Raw layer'")

    val dsRawSourcePath = ConstructorPaths.buildRawPath(source, dataset.dataset)
    // Retorna una lista vacía en caso de que partition_by sea NULL en la BD
    val partitions = Option(dataset.delta_column).map(_.split(",").map(_.trim).toList).getOrElse(List.empty[String])

    createDatabase(s"${CommonsConstants.RAW_SOURCES}.$source") match {
      case Right(schemaName) =>
        dataset.extract_type match {
          case IngestionConstants.FULL_PARTITION_EXTRACTION =>
            logger.info(s"Auto Loader Component Write FULL_PARTITION Table $dataset in raw_source layer, source $source, path $dsRawSourcePath")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite,
              partitionBy = partitions, otherOptions = Map("partitionOverwriteMode"-> "dynamic"))
            AutoLoader.sparkWriteStream(dataset, dataFrame, dsRawSourcePath, sparkOptions, source)
          case IngestionConstants.FULL_EXTRACTION =>
            logger.info(s"Auto Loader Component Write FULL Table $dataset in raw_source layer, source $source, path $dsRawSourcePath")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite)
            AutoLoader.sparkWriteStream(dataset, dataFrame, dsRawSourcePath, sparkOptions, source)
          case IngestionConstants.DELTA_EXTRACTION =>
            logger.info(s"Auto Loader Component Write DELTA Table $dataset in raw_source layer, source $source, path $dsRawSourcePath")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Append,
              partitionBy = partitions)
            AutoLoader.sparkWriteStream(dataset, dataFrame, dsRawSourcePath, sparkOptions, source)
          case IngestionConstants.STREAM_EXTRACTION =>
            logger.info(s"Auto Loader Component Write DELTA Table $dataset in raw_source layer, source $source, path $dsRawSourcePath")
            val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Append,
              partitionBy = partitions)
            AutoLoader.sparkWriteStream(dataset, dataFrame, dsRawSourcePath, sparkOptions, source)
          case IngestionConstants.CDC_EXTRACTION =>
            logger.info(s"Write DataFrame cleaned to 'raw sources' layer: ${dataset.dataset} in CDC Mode")

            logger.info(s"Filling 'Null' values in key columns: ${dataset.key_columns}")
            val dfFillNulls = dataFrame.na.fill("", dataset.key_columns.split(",").toSeq)

            if(DeltaTable.isDeltaTable(SparkConfiguration.spark, dsRawSourcePath)) {
              logger.info(s"The Delta Table exists: Execute Merge Delta Table with Ingestion module in $dsRawSourcePath")
              AutoLoader.sparkWriteStreamMerge(dataset, dfFillNulls, dsRawSourcePath, source)
              //println(s"Auto Loader Component Write CDC (merge) Table $dataset in raw_source layer, source $source, path $dsRawSourcePath")
            } else {
              logger.info(s"The Delta Table not exists: Execute Spark Write Data 'saveAsTable' with Quality module in $dsRawSourcePath")
              val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Append,
                partitionBy = partitions)
              AutoLoader.sparkWriteStream(dataset, dfFillNulls, dsRawSourcePath, sparkOptions, source)
              //println(s"Auto Loader Component Write CDC (Create) Table $dataset in raw_source layer, source $source, path $dsRawSourcePath")
            }
        }
      case Left(exception) =>
        throw new IngestionManagerException(s"Error creating schema: ${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", exception)
    }
  }


  /**
   *
   * @return
   */
  def finishTasksIngestion(): String = {
    logger.info("End AutoLoader Databricks Job")
    val message =
      """
      ####    ####   ####         ####   ############     ########    ############   ####     ########
      ####    ####     ####      ####    ####    ####   ####    ####      ####       ####   ####    ####
      ############       ####  ####      ####    ####   ####    ####      ####              ####    ####
      ############          ####         ############   ############      ####       ####   ############
      ####    ####          ####         ####           ####    ####      ####       ####   ####    ####
      ####    ####          ####         ####           ####    ####      ####       ####   ####    ####
      ####    ####          ####         ####           ####    ####      ####       ####   ####    ####

      ##########  ###    ###  ###########  ##########  ###       ##########  ##########  #####     #######  #########
      ###    ###  ###    ###      ###      ###    ###  ###       ###    ###  ###    ###  #    ###  ###      ###    ##
      ###    ###  ###    ###      ###      ###    ###  ###       ###    ###  ###    ###  #    ###  #######  ###    ##
      ##########  ###    ###      ###      ###    ###  ###       ###    ###  ##########  #    ###  ###      #########
      ###    ###  ###    ###      ###      ###    ###  ###       ###    ###  ###    ###  #    ###  ###      #####
      ###    ###  ##########      ###      ##########  ########  ##########  ###    ###  #####     #######  ###  #####

      ########  ####  ########  ####  ####  ############  ####    ####  ####
      ####            ####  ##  ####        ########      ####    ####  ####
      ########  ####  ####  ##  ####  ####  ############  ############  ####
      ####      ####  ####  ##  ####  ####         #####  ############  ####
      ####      ####  ####  ##  ####  ####  ############  ####    ####
      ####      ####  ####  ########  ####  ############  ####    ####  ####
  """

    println(message)

    "End AutoLoader Databricks Job"
  }

}
