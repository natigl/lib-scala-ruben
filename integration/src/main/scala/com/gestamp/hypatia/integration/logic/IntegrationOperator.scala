package com.gestamp.hypatia.integration.logic

import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.databricks.deltalake.{DeltaTablesManager, DeltaChangeDataFeed}
import com.gestamp.hypatia.integration.exception.IntegrationTaxonomyException
import com.gestamp.hypatia.integration.impl.HistoricalIntegrationManager.datasetDao
import com.gestamp.hypatia.integration.operations.TaxonomyOperations
import com.gestamp.hypatia.integration.utils.{Config, ConstantsIntegration, DatasetTag, Stage, Taxonomy}
import com.gestamp.hypatia.postgresql.model._
import com.gestamp.hypatia.spark.SparkConfiguration
import com.gestamp.hypatia.utils.CommonsConstants
import com.gestamp.hypatia.integration.utils.ConstantsIntegration._
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util._


class IntegrationOperator(historicalLoad: Boolean = false, deltaVersion: Option[Int] = None) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)


  /**
   * Method to start to process JSON Taxonomy definition
   *
   * @param config
   * @param source
   * @param distillation
   * @return
   */
  def applyOperation(config: Config, source: String, distillation: Distillation): DataFrame = {
    logger.info(s"Start applying historical taxonomy method for distillation dataset ${distillation.datum}")

    if (config.taxonomy != null) {
      Try(processOperation(config.taxonomy, source, distillation)) match {
        case Success(df) => df
        case Failure(e) =>
          throw new IntegrationTaxonomyException(s"Error applying taxonomy for distillation dataset ${distillation.datum} => ${e.getMessage}\n" +
            s"Stack trace: ${e.getStackTrace.mkString("\n")}", e)
      }
    } else
      throw new IntegrationTaxonomyException("Taxonomy tag was not encountered in JSON Marketplace after parsing")
  }

  /**
   * Method that executes the values of the taxonomy tag from the JSON String
   * - The values under the taxonomy tag are extracted
   * - Get the dataframes from left and right tables. This tables could be recursives.
   * - The operation specified is executed
   * - Finally run select statement
   *
   * @param taxonomy
   * @param source
   * @param distillation
   * @return
   */
  private def processOperation(taxonomy: Taxonomy, source: String, distillation: Distillation): DataFrame = {

    val operationTag = taxonomy.operation
    val selectCDF =
      if (historicalLoad && deltaVersion.isEmpty) false
      else distillation.ingest_type.equalsIgnoreCase(CDC_EXTRACTION)
    logger.info(s"selectCDF: $selectCDF")

    logger.info(s"Taxonomy: left table stage process for distillation dataset ${distillation.datum}")
    val dfLeftTable = processStage(taxonomy.leftTable, source, distillation, selectCDF)
    logger.info(s"Taxonomy: right table stage process for distillation dataset ${distillation.datum}")
    val dfRightTable = processStage(taxonomy.rightTable, source, distillation, selectCDF)

    val exprTag = taxonomy.expr
    val selectTag = taxonomy.select

    val dfOperation = processOperation(operationTag, exprTag, Seq(dfLeftTable, dfRightTable))

    processSelect(s"final_table_${distillation.datum}", selectTag, dfOperation, selectCDF)
  }


  /**
   * Method that executes the values of the stage tag from the JSON String.
   * - This method could be recursive, depends on JSON definition. Both left table and right table could be either
   * a stage or a dataset.
   * - In the case left table or right table are a stage, the same method is called again until a dataset is found.
   * - In the case left table or right table are a dataset, the select statement is executed.
   *
   * @param stage
   * @param source
   * @param distillation
   * @param selectCDF
   * @return
   */
  private def processStage(stage: Stage, source: String, distillation: Distillation, selectCDF: Boolean): DataFrame = {
    logger.info(s"Taxonomy: Processing stage")

    if (stage.dataset != null) {
      val dfRawClean = processDataset(stage.dataset, source)
      processSelect(s"${stage.dataset.name}_${distillation.datum}", stage.dataset.select, dfRawClean, selectCDF)

    } else if (stage.dataset == null && (stage.leftTable != null || stage.rightTable != null)) {

      val operationTag = stage.operation
      val dfLeftTable = processStage(stage.leftTable, source, distillation, selectCDF)
      val dfRigtTable = processStage(stage.rightTable, source, distillation, selectCDF)
      val exprTag = stage.expr
      val selectTag = stage.select

      val dfOperationStage = processOperation(operationTag, exprTag, Seq(dfLeftTable, dfRigtTable))
      processSelect(s"stage_table_${distillation.datum}", selectTag, dfOperationStage, selectCDF)
    } else {
      SparkConfiguration.spark.emptyDataFrame
    }
  }


  /**
   * Method that executes the values of the dataset tag from the JSON String
   * - First, we look for the dataset id inside the input parameters.
   * - Then, we read total historical data from that delta table.
   *
   * @param dataset
   * @param source
   * @return
   */
  private def processDataset(dataset: DatasetTag, source: String): DataFrame = {
    logger.info(s"Taxonomy: Processing read for ${dataset.name} dataset")
    val ds = datasetDao.getDatasetByName(dataset.name)
    val extractType = ds.extract_type.toUpperCase
    if (historicalLoad) {
      logger.info(s"The taxonomy is being applying for a historical processing")
      deltaVersion match {
        case Some(dv) =>
          logger.info(s"There's a delta version specified for reading: $dv")

          val readResult = extractType match {
            case ConstantsIntegration.FULL_PARTITION_EXTRACTION |
                 ConstantsIntegration.DELTA_EXTRACTION |
                 ConstantsIntegration.STREAM_EXTRACTION |
                 ConstantsIntegration.FULL_EXTRACTION =>
              DeltaChangeDataFeed.readDeltaFeedFromVersion(CommonsConstants.RAW_CLEAN, source, ds.dataset, dv)

            case ConstantsIntegration.CDC_EXTRACTION =>
              DeltaChangeDataFeed.readDeltaFeedFromVersionCDC(CommonsConstants.RAW_CLEAN, source, ds.dataset, dv)

            case _ =>
              throw new IntegrationTaxonomyException(s"Invalid extract type: $extractType")
          }

          readResult match {
            case Right(dfRawClean) => dfRawClean
            case Left(e) =>
              throw new IntegrationTaxonomyException(
                s"Error trying to read a specific delta version from Raw clean ($extractType extract type): ${e.getMessage}", e
              )
          }

        case None =>
          logger.info("There's no delta version specified for reading.")

          DeltaTablesManager.readDeltaTable(CommonsConstants.RAW_CLEAN, source, dataset.name) match {
            case Right(dfRawClean) => dfRawClean
            case Left(e) =>
              throw new IntegrationTaxonomyException(s"Error trying to read entire Delta Table from Raw clean: ${e.getMessage}", e)
          }
      }
    } else {
      val readDeltaVersion = DeltaChangeDataFeed.getLatestSignificantDeltaVersion(CommonsConstants.RAW_CLEAN, source, ds.dataset)
      extractType match {
        case ConstantsIntegration.FULL_PARTITION_EXTRACTION | ConstantsIntegration.DELTA_EXTRACTION | ConstantsIntegration.STREAM_EXTRACTION =>
          DeltaChangeDataFeed.readDeltaFeedFromVersion(CommonsConstants.RAW_CLEAN, source, dataset.name, readDeltaVersion) match {
            case Right(dfRawSource) => dfRawSource
            case Left(e) =>
              throw new IntegrationTaxonomyException(s"Error trying to read latest delta version from Raw clean ($extractType extract type): ${e.getMessage}", e)
          }
        case ConstantsIntegration.CDC_EXTRACTION =>
          DeltaChangeDataFeed.readDeltaFeedFromVersionCDC(CommonsConstants.RAW_CLEAN, source, dataset.name, readDeltaVersion) match {
            case Right(dfRawSource) => dfRawSource
            case Left(e) =>
              throw new IntegrationTaxonomyException(s"Error trying to read latest delta version from Raw clean ($extractType extract type): ${e.getMessage}", e)
          }
        case ConstantsIntegration.FULL_EXTRACTION =>
          DeltaTablesManager.readDeltaTable(CommonsConstants.RAW_CLEAN, source, dataset.name) match {
            case Right(dfRawClean) => dfRawClean
            case Left(e) =>
              throw new IntegrationTaxonomyException(s"Error trying to read entire Delta Table from Raw clean: ${e.getMessage}", e)
          }
        case _ =>
          throw new IntegrationTaxonomyException(s"Invalid extract type: $extractType")
      }
    }
  }


  /**
   * Method that executes the values of the select tag from the JSON String
   * - Create a temporal view.
   * - Execute SQL statement select against temporal view.
   *
   * @param table
   * @param select
   * @param dataFrame
   * @param selectCDF
   * @return
   */
  private def processSelect(table: String, select: String, dataFrame: DataFrame, selectCDF:Boolean): DataFrame = {
    logger.info(s"Taxonomy: select tag process")
    dataFrame.createOrReplaceGlobalTempView(s"tmpView_$table")

    if (SparkConfiguration.spark.catalog.tableExists(s"global_temp.tmpView_$table")) {
      logger.info(s"tmpView_$table was created and exists")
    } else logger.info(s"tmpView_$table was not found")

    val selectEscaped = selectCDF match {
      case true =>
        if (select == "*") select
        else select.concat(",_change_type,_commit_version,_commit_timestamp").split(",").map(name =>
          if (name.contains(" AS ")) name.replaceAll("""(.*)\s+AS\s+(.*)""", "`$1` AS `$2`")
          else "`" + name.trim + "`").mkString(",")
      case false =>
        val columnsToExclude = Seq("_change_type", "_commit_version", "_commit_timestamp")
        val existingColumnsToExclude = columnsToExclude.filter(dataFrame.columns.contains(_))
        select match {
          case "*" if existingColumnsToExclude.nonEmpty => select.concat(" EXCEPT(_change_type, _commit_version, _commit_timestamp)")
          case "*" if existingColumnsToExclude.isEmpty => select
          case _ => select.split(",").map {
            case name if name.contains(" AS ") => name.replaceAll("""(.*)\s+AS\s+(.*)""", "`$1` AS `$2`")
            case name => "`" + name.trim + "`"
          }.mkString(",")
        }
    }

    logger.info(s"Taxonomy select: SELECT $selectEscaped FROM global_temp.tmpView_$table")

    Try(SparkConfiguration.spark.sql(s"SELECT $selectEscaped FROM global_temp.tmpView_$table")) match {
      case Success(df) =>
        logger.info(s"Successful execution for 'SELECT $selectEscaped FROM global_temp.tmpView_$table' query.")
        SparkConfiguration.spark.catalog.dropGlobalTempView(s"tmpView_$table")
        df
      case Failure(e) =>
        throw new IntegrationTaxonomyException(s"Marketplace JSON taxonomy wrong. " +
          s"Error executing 'SELECT $selectEscaped FROM global_temp.tmpView_$table' => ${e.getMessage}", e)
    }
  }


  /**
   * Method that executes the values of the operation tag from the JSON String
   *
   * @param joinType
   * @param expr
   * @param dataFrames
   * @return
   */
  private def processOperation(joinType: String, expr: String, dataFrames: Seq[DataFrame]): DataFrame = {
    logger.info(s"Taxonomy: operation tag process")

    joinType.toLowerCase match {
      case op if JOIN_OPERATIONS.contains(op) => TaxonomyOperations.joinOperation(joinType, expr, dataFrames)
      case OPERATION_UNIONBYNAME => TaxonomyOperations.unionByNameOperation(dataFrames)
      case OPERATION_UNION => TaxonomyOperations.unionOperation(dataFrames)
      case NO_OPERATION => dataFrames.head
      case _ => throw new Exception(s"Invalid operation: $joinType")
    }
  }
}

  object IntegrationOperator {
    /**
     * Aplica la taxonomía (union/join de datasets de raw_clean) definida en la
     * configuración, en modo normal o histórico (con versión Delta opcional).
     *
     * @param config         configuración de la taxonomía
     * @param source         source de los datos
     * @param distillation   distillation destino
     * @param historicalLoad true para leer el histórico completo en vez del incremento
     * @param deltaVersion   versión Delta concreta a leer, si aplica
     * @return el DataFrame resultante de la operación
     */
    def applyOperation(config: Config, source: String, distillation: Distillation, historicalLoad: Boolean = false, deltaVersion: Option[Int] = None): DataFrame = new IntegrationOperator(historicalLoad, deltaVersion).applyOperation(config, source, distillation)
  }

