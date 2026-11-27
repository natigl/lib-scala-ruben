package com.gestamp.hypatia.impl

import com.gestamp.hypatia.adls.ADLS2Manager.setADLS2OAuthSparkConfiguration
import com.gestamp.hypatia.args.HistoricalIngestionJobParams
import com.gestamp.hypatia.historical.HistoricalIngestionFactory
import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.spark.SparkConfiguration
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.{deleteDeltaTable, readDeltaTable}
import com.gestamp.hypatia.exceptions.HistoricalIngestionManagerException
import com.gestamp.hypatia.utils.{CommonsConstants, IngestionConstants}
import com.gestamp.hypatia.impl.IngestionManager.readNewFiles
import io.delta.tables.DeltaTable

import org.apache.spark.sql.functions.{col, coalesce}
import scala.util.{Failure, Success, Try}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Orquestador de la carga histórica: para cada dataset borra checkpoint y datos
 * previos, relee todo el landing y lo escribe en raw_sources con el loader que
 * corresponda a su tipo de extracción.
 */
object HistoricalIngestionManager {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Ejecuta la carga histórica de todos los datasets recibidos: resuelve el loader
   * por tipo de extracción, borra el checkpoint, aplica coalesce de columnas
   * renombradas si se pidió, filtra las columnas al esquema de la tabla destino
   * si ya existe y escribe el histórico en raw_sources.
   *
   * @param jobParams parámetros del job (source, datasets y columnas a coalescer)
   * @return mensaje de fin del proceso
   */
  def handleData(jobParams: HistoricalIngestionJobParams) = {
    logger.info(s"Start Historical Loader, datasets: ${jobParams.datasets.toString}")

    setADLS2OAuthSparkConfiguration()

    jobParams.datasets.foreach(ds => {
      val loader = HistoricalIngestionFactory.createHistoricalLoader(ds)

      val dsRawSourcePath = ConstructorPaths.buildRawPath(jobParams.source, ds.dataset)

      Try(loader.getSortedLandingDirectories(jobParams.source, ds.dataset)) match {
        case Success(directoriesToProcess) =>
          loader.deleteCheckpoint(jobParams.source, ds.dataset)
          readNewFiles(jobParams.source, ds) match {
            case Right(dataFrame) =>
              val coalescedDf = if(jobParams.coalescedColumns.nonEmpty){
                logger.info(s"Coalesced columns: ${jobParams.coalescedColumns}")
                jobParams.coalescedColumns.foldLeft(dataFrame) { (accDF, pair) =>
                  accDF
                    .withColumn(pair.current_column_name, coalesce(col(pair.current_column_name), col(pair.old_column_name)))
                    .drop(pair.old_column_name)
                }
              } else dataFrame

              val filteredDf = if (DeltaTable.isDeltaTable(SparkConfiguration.spark, dsRawSourcePath)) {
                deleteDeltaTable(CommonsConstants.RAW_SOURCES, jobParams.source, ds.dataset)
                loader.deleteRawSourcesFiles(jobParams.source, ds.dataset)
                val targetTable = readDeltaTable(CommonsConstants.RAW_SOURCES, jobParams.source, ds.dataset) match {
                  case Right(dfRawSources) => dfRawSources
                  case Left(exception) =>
                    throw new IllegalArgumentException("Error running DeltaTableManager read Delta Table", exception)
                }
                val targetColumns = if (jobParams.source == IngestionConstants.SAP_ECC_SOURCE) {
                  val sapEccColumns = Set("ODQ_CHANGEMODE", "ODQ_ENTITYCNTR", "_PACKAGEID", "_SEQUENCENUMBER")
                  targetTable.columns.toSet ++ sapEccColumns
                } else targetTable.columns.toSet
                coalescedDf.selectExpr(coalescedDf.columns.filter(targetColumns.contains).map(colName => s"`$colName`"): _*)
              } else coalescedDf
              loader.writeHistoricalDfToRawSource(filteredDf, jobParams.source, ds, dsRawSourcePath, directoriesToProcess)
            case Left(exception) =>
              throw new Exception(exception.getMessage, exception)
          }

        case Failure(e) =>
          throw new HistoricalIngestionManagerException(s"No directories were found in Landing for ${ds.dataset} table. " +
            s"For this reason, nothing was done to the ${ds.dataset} table in raw_sources layer. No data has been written. " +
            s"The error was the following: ${e.getMessage}", e)
      }
    })
    "End handleData Historical Loader"
  }

  /**
   * Marca el fin del job de carga histórica (solo log).
   *
   * @return mensaje de fin del job
   */
  def finishTasksHistoricalIngestion(): String = {
    logger.info("End Historical Loader Databricks Job")
    "End Historical Loader Databricks Job"
  }
}
