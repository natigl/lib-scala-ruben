package com.gestamp.hypatia

import com.gestamp.hypatia.adls.ADLS2Manager.sparkWriteData
import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.args.JobParamsCase
import com.gestamp.hypatia.databricks.deltalake.{DeltaChangeDataFeed, DeltaTablesManager}
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.createDatabase
import com.gestamp.hypatia.exceptions.HypatiaManagerException
import com.gestamp.hypatia.postgresql.dao.{DomainDao, SubdomainDao, TagExclusionDao}
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import com.gestamp.hypatia.utils.CommonsConstants.{ENV, ENV_SOURCE}
import com.gestamp.hypatia.utils.tag_propagation.{DatabricksSystemTableAccessor, TableRef, TagPropagationService}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object HypatiaDatasetManager {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * Generates the ADLS path for the Gold Layer, using the provided JobParamsCase object.
   * The generated path is constructed based on the name of the use case contained in the JobParamsCase.
   *
   * @param jobParams the JobParamsCase object containing the necessary information to generate the ADLS path.
   * @return a String representing the ADLS path for the Gold Layer.
   * @throws HypatiaManagerException if an error occurs while building the ADLS path for the Gold Layer.
   */
  def generatePathGoldLayer(jobParams: JobParamsCase): String = {
    logger.info(s"Start to build ADLS Path: Gold Layer")
    Try(
      ConstructorPaths.buildGoldUseCasePath(jobParams.use_case.name)
    ) match {
      case Success(goldPath) => goldPath
      case Failure(exception) =>
        throw new HypatiaManagerException(s"Error to build Gold layer path", exception)
    }
  }

  /**
   * Reads a DataFrame from Azure Data Lake Storage (ADLS) using the specified catalog, schema, and table distillation.
   *
   * @param path The table distillation information for the DataFrame to be read.
   * @return The DataFrame read from the specified path in ADLS.
   * @throws Exception If there's an error while reading the DataFrame from ADLS.
   *
   *                   This function takes catalog, schema, and table distillation information as input and constructs the full ADLS path.
   *                   Then, it attempts to read the DataFrame from the specified path using the SparkConfiguration.
   *                   If the reading is successful, the DataFrame is returned. If there's an error, the function logs the error and throws an exception.
   */
  def readDataFromDeltaTable(catalog: String, schema: String, deltaObj: String): DataFrame = {
    logger.info(s"HypatIa Dataset Manager: Reading data from path: $catalog, $schema, $deltaObj")

    DeltaTablesManager.readDeltaTable(catalog, schema, deltaObj) match {
      case Right(df) => df
      case Left(exception) => Try(readDataFromView(catalog, schema, deltaObj)) match {
        case Success(value) => value
        case Failure(exception) => throw exception
      }
    }

  }

  /**
   * Reads a DataFrame from a View using the specified catalog, schema, and table distillation.
   *
   * @param catalog Catalog of the data
   * @return The DataFrame read from the view
   * @throws Exception If there's an error while reading the DataFrame from ADLS.
   *
   *                   This function takes catalog, schema, and table distillation information as input and constructs the full ADLS path.
   *                   Then, it attempts to read the DataFrame from the specified path using the SparkConfiguration.
   *                   If the reading is successful, the DataFrame is returned. If there's an error, the function logs the error and throws an exception.
   */
  def readDataFromView(catalog: String, schema: String, deltaObj: String): DataFrame = {
    logger.info(s"HypatIa Dataset Manager: Reading data from path: $catalog, $schema, $deltaObj")

    val viewName = s"$catalog.$schema.$deltaObj"
    Try(SparkConfiguration.spark.table(viewName)) match {
      case Success(df) => df
      case Failure(_) => throw new HypatiaManagerException(s"Error getting info from view $viewName")
    }

  }

  /**
   * Validates the save mode of a write to the Gold Layer. Only Overwrite and Append are supported.
   * The table is never dropped before writing: an existing table is written by name (see
   * ADLS2Manager.sparkWriteData), which is compatible with row filters / column masks and keeps
   * its tags, row filters, grants and Delta history.
   *
   * @param saveMode The Spark save mode of the write
   * @throws HypatiaManagerException if the save mode is not supported
   */
  private[hypatia] def validateSaveMode(saveMode: SaveMode): Unit = saveMode match {
    case SaveMode.Overwrite | SaveMode.Append => ()
    case other => throw new HypatiaManagerException(
      s"Unsupported SaveMode $other: only Overwrite and Append are supported for Gold layer")
  }

  private val OverwriteSchemaOption: String = "overwriteSchema"

  /**
   * In Overwrite mode adds `overwriteSchema=true` to the write options if they do not set it: the table is no longer
   * dropped before writing, so a schema or partitioning change needs it (the DROP used to allow it). An explicit
   * value is kept (and logged if it is not "true"). Append options are returned unchanged.
   *
   * @param sparkOptions Spark write options
   * @return the options to use in the write
   */
  private[hypatia] def withOverwriteSchema(sparkOptions: SparkOptions): SparkOptions = {
    if (sparkOptions.saveMode != SaveMode.Overwrite) {
      sparkOptions
    } else {
      // Spark options are case-insensitive: any capitalization of the key counts as the same option
      val existing = sparkOptions.otherOptions.find { case (key, _) =>
        key != null && key.equalsIgnoreCase(OverwriteSchemaOption)
      }

      existing match {
        case Some((key, value)) if value != null =>
          if (!value.trim.equalsIgnoreCase("true"))
            logger.warn(s"Overwrite mode with explicit $key=$value: kept, a schema or " +
              s"partitioning change will make the write fail")
          sparkOptions
        case _ =>
          // Missing option, or present with a null value (treated as missing and replaced)
          logger.info(s"Overwrite mode: adding $OverwriteSchemaOption=true to the write options")
          val otherOptions = existing.map { case (key, _) => sparkOptions.otherOptions - key }
            .getOrElse(sparkOptions.otherOptions)
          sparkOptions.copy(otherOptions = otherOptions + (OverwriteSchemaOption -> "true"))
      }
    }
  }

  /**
   * This method calls the ADLS Gen2 to write a dataframe.
   * In Overwrite mode the table content is replaced; in Append mode the rows are added to the existing table.
   * The table is not dropped in any mode, so its row filters, tags, grants and history are kept.
   *
   * @param databaseName  The database name in format "catalog.schema"
   * @param datasetName   The table name
   * @param dataFrame     The DataFrame to write
   * @param path          The path where to write the data
   * @param sparkOptions  Spark write options (saveMode must be Overwrite or Append)
   * @return
   */
  def writeDataToDeltaTable(databaseName: String, datasetName: String, dataFrame: DataFrame, path: String, sparkOptions: SparkOptions): Unit = {
    logger.info(s"Writing data to Azure Data Lake Gen2, path = $path, saveMode = ${sparkOptions.saveMode}")

    validateSaveMode(sparkOptions.saveMode)

    val writeOptions = withOverwriteSchema(sparkOptions)

    createDatabase(databaseName) match {
      case Right(dbName) => {
        val fullName = s"$dbName.$datasetName"

        if (writeOptions.saveMode == SaveMode.Append && writeOptions.otherOptions.contains(OverwriteSchemaOption))
          logger.warn(s"Option overwriteSchema is ignored in Append mode for table $fullName")

        // Tabla nueva: su linaje aún no está en system.access.column_lineage y se lee por la API REST.
        // Si no se puede comprobar, se trata como nueva (la API cae a la tabla de sistema si falla).
        val tableIsNew = !Try(SparkConfiguration.spark.catalog.tableExists(fullName)).getOrElse(false)

        sparkWriteData(fullName, dataFrame, path, "saveAsTable", writeOptions) match {
          case Right(_) => {
            logger.info(s"The data was successfully written in the $path path")

            // Propagate tags from silver layer after a successful write
            propagateTagsToTable(databaseName, datasetName, sparkOptions.saveMode, tableIsNew)
          }
          case Left(exception) => {
            logger.error(s"Error Writing Data Lake on path: $path. Error: ${exception}")
            throw exception
          }
        }
      }
      case Left(exception) => {
        logger.error(s"Error creating Database: $datasetName")
        throw exception
      }
    }
  }

  /**
   * Propagates tags from silver layer to the target table.
   *
   * - Overwrite: the column tags of the table are synchronized with the desired state, i.e. the tags the table
   *   would get after DROP + create + propagation (TagPropagationService.syncTagsToDesiredState): first the
   *   missing / changed tags are SET and only then the obsolete ones are UNSET, so the table never has less
   *   filter than it should. A table in the exclusion list must end up without column tags (as after the DROP).
   *   If the desired state cannot be computed, it falls back to the additive propagation (nothing is removed).
   * - Append: additive propagation, as before (only tags whose key the column does not have yet are added).
   *
   * If tag propagation fails, logs a warning but doesn't fail the operation.
   *
   * @param databaseName The database name in format "catalog.schema"
   * @param tableName    The table name
   * @param saveMode     The save mode of the write (Overwrite or Append)
   * @param tableIsNew   true if the table did not exist before this write: its lineage is read from the lineage REST
   *                     API; otherwise from system.access.column_lineage (lineage of previous executions)
   */
  private def propagateTagsToTable(databaseName: String, tableName: String, saveMode: SaveMode,
                                   tableIsNew: Boolean): Unit = {
    Try {
      // Parse catalog and schema from databaseName (format: "catalog.schema")
      val parts = databaseName.split("\\.")
      if (parts.length >= 2) {
        val catalog = parts(0)
        val schema = parts(1)
        val silverLayer = "semantic" + ENV_SOURCE

        // La lista de exclusión se compara sin distinguir mayúsculas: Unity Catalog guarda el nombre de tabla en
        // minúsculas, así que la exclusión debe casar venga como venga la caja del nombre registrado.
        val excludedTables: Set[String] = new TagExclusionDao().getActiveExclusions.map(e => TableRef.normalize(e.table_name)).toSet
        val excluded = excludedTables.contains(TableRef.normalize(tableName))
        val lineageMsg = s"[Lineage] Lineage of $catalog.$schema.$tableName read from " +
          (if (tableIsNew) "the lineage REST API (new table)" else "system.access.column_lineage (existing table)")
        logger.info(lineageMsg)
        println(lineageMsg) // el log de la aplicación llega incompleto a la salida del run
        val tagService = new TagPropagationService(SparkConfiguration.spark,
          new DatabricksSystemTableAccessor(SparkConfiguration.spark, useLineageApi = tableIsNew))

        if (saveMode == SaveMode.Overwrite) {
          logger.info(s"Overwrite mode: synchronizing tags of table $catalog.$schema.$tableName with the desired " +
            s"state (excluded = $excluded)")

          Try(tagService.syncTagsToDesiredState(catalog, schema, tableName, silverLayer, daysBack = 30,
            excluded = excluded, dryRun = false)) match {
            case Success(results) => logTagResults(results)
            case Failure(e) =>
              logger.warn(s"Could not compute the desired tag state of $catalog.$schema.$tableName: ${e.getMessage}. " +
                s"Falling back to additive tag propagation, obsolete tags are kept", e)
              if (!excluded) propagateTagsAdditive(tagService, catalog, schema, tableName, silverLayer)
          }
        } else if (excluded) {
          logger.info(s"Table $catalog.$schema.$tableName is in tag exclusion list, skipping tag propagation")
        } else {
          propagateTagsAdditive(tagService, catalog, schema, tableName, silverLayer)
        }
      } else {
        logger.warn(s"Could not parse catalog and schema from databaseName: $databaseName. Expected format: catalog.schema")
      }
    } match {
      case Success(_) => // Tag propagation succeeded or had no tags to propagate
      case Failure(exception) =>
        logger.warn(s"Tag propagation failed for table $databaseName.$tableName: ${exception.getMessage}. Table was written successfully but its tags may not be up to date.", exception)
    }
  }

  /**
   * Additive tag propagation: adds the silver tags whose key the gold column does not have yet. Nothing is removed.
   */
  private def propagateTagsAdditive(tagService: TagPropagationService, catalog: String, schema: String,
                                    tableName: String, silverLayer: String): Unit = {
    logger.info(s"Starting tag propagation for table $catalog.$schema.$tableName from silver layer")

    val propagationPlan = tagService.getTagPropagationPlan(
      goldCatalog = catalog,
      goldSchema = schema,
      goldTable = tableName,
      silverLayer = silverLayer,
      daysBack = 30
    )

    if (propagationPlan.nonEmpty) {
      logger.info(s"Found ${propagationPlan.size} columns with tags to propagate")
      logTagResults(tagService.executeTagPropagation(propagationPlan, dryRun = false))
    } else {
      logger.info(s"No tags found to propagate for table $catalog.$schema.$tableName")
    }
  }

  private def logTagResults(results: Seq[(String, Boolean, String)]): Unit = {
    val successCount = results.count(_._2)
    val failureCount = results.size - successCount

    logger.info(s"Tag propagation completed: $successCount successful, $failureCount failed")
    println(s"[Tags] Tag propagation completed: $successCount successful, $failureCount failed")

    results.foreach {
      case (sql, true, _)    => logger.info(s"Tag propagation success: $sql"); println(s"[Tags] OK $sql")
      case (sql, false, msg) => logger.warn(s"Tag propagation failed: $sql - $msg"); println(s"[Tags] FAILED $sql - $msg")
    }
  }

}
