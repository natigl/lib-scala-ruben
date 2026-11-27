package com.gestamp.hypatia.logic

import com.gestamp.hypatia.utils.{CoalescePair, CommonsConstants}
import com.gestamp.hypatia.adls.ConstructorPaths
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager
import com.gestamp.hypatia.postgresql.dao._
import com.gestamp.hypatia.postgresql.model._
import com.gestamp.hypatia.spark.SparkOptions
import com.gestamp.hypatia.adls.ADLS2Manager.sparkWriteData
import com.gestamp.hypatia.integration.impl.HistoricalIntegration
import com.gestamp.hypatia.impl.{HistoricalIngestion, HistoricalQuality}
import com.gestamp.hypatia.integration.utils.SchemaCsvFileReader.castDataFrameByCSVSchema
import com.gestamp.hypatia.integration.args.IntegrationJobParams
import com.gestamp.hypatia.args.{HistoricalIngestionJobParams, HistoricalQualityJobParams}
import com.gestamp.hypatia.integration.args.HistoricalIntegrationJobParams
import com.gestamp.hypatia.exceptions.LakehouseDefaultException
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.SaveMode

import scala.util.{Failure, Left, Right, Success, Try}

/**
 * Implementación de las acciones de lakehouse: recargas históricas por capa y
 * operaciones de evolución de esquema sobre tablas Delta.
 */
class LakehouseDefault {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Deja constancia en el log de qué fila de dataset se ha resuelto para el source dado.
   * El nombre de dataset no es único (puede repetirse entre sources), así que se logean
   * id y group para poder verificar que se ha cogido el dataset esperado.
   *
   * @param source  source con el que se ha resuelto
   * @param dataset dataset resuelto
   */
  private def logResolvedDataset(source: String, dataset: Dataset): Unit =
    logger.info(s"Resolved 1 dataset for source '$source': ${dataset.dataset}#${dataset.id}(group=${dataset.group})")

  /**
   * Equivalente a logResolvedDataset para las data relationships de la capa integration.
   *
   * @param source        source con el que se han resuelto
   * @param relationships relaciones resueltas
   */
  private def logResolvedDataRelationships(source: String, relationships: Seq[DataRelationship]): Unit =
    logger.info(s"Resolved ${relationships.size} data relationship(s) for source '$source': " +
      relationships.map(dr => s"${dr.dataset_name}#${dr.dataset_id} -> ${dr.distillation_name}").mkString(", "))

  /**
   * Recarga la tabla con todo el histórico en la capa indicada: raw_sources
   * (ingesta histórica), raw_clean (calidad histórica) o integration
   * (integración histórica). El dataset se resuelve por (nombre, source).
   *
   * @param table            tabla a recargar
   * @param layer            capa destino de la recarga
   * @param source           source del dataset
   * @param coalescedColumns pares de columnas renombradas a coalescer (solo raw_sources)
   */
  def total_historical_reload(table: String, layer:String, source: String, coalescedColumns: List[CoalescePair] = List.empty): Unit = {
    logger.info(s"Reloading $table table with full historical data in $layer layer")

    Try {
      layer.toLowerCase match {
        case CommonsConstants.RAW_SOURCES =>
          val datasetDao: DatasetDao = new DatasetDao
          val dataset = datasetDao.getDatasetByName(table, source)
          logResolvedDataset(source, dataset)

          HistoricalIngestion.startHistoricalIngestion(HistoricalIngestionJobParams(source, Seq(dataset), coalescedColumns))

        case CommonsConstants.RAW_CLEAN =>
          val datasetDao: DatasetDao = new DatasetDao
          val dataset = datasetDao.getDatasetByName(table, source)
          logResolvedDataset(source, dataset)

          HistoricalQuality.startHistoricalQuality(HistoricalQualityJobParams(source, Seq(dataset)))

        case CommonsConstants.INTEGRATION =>
          val datarelationshipDao: DataRelationshipDao = new DataRelationshipDao
          val distillations = datarelationshipDao.getDataRelationshipByDatasetName(Seq(table), source)
          logResolvedDataRelationships(source, distillations)

          HistoricalIntegration.startHistoricalIntegration(HistoricalIntegrationJobParams(source, distillations))
      }
    } match {
      case Success(result) =>
        logger.info(s"$result")
        logger.info(s"Total historical reloading of the table $table, in $layer layer, has finished successfully")
      case Failure(e) =>
        logger.error(s"Something went wrong when trying to reload table $table in $layer layer with all historical data", e.getMessage)
        throw new LakehouseDefaultException(s"Error when trying to reload table $table in $layer layer with all historical data", e)
    }
  }

  /**
   * Recarga la tabla en la capa indicada con una versión Delta concreta de la
   * capa anterior (solo raw_clean e integration). El dataset se resuelve por
   * (nombre, source).
   *
   * @param table         tabla a recargar
   * @param layer         capa destino de la recarga (raw_clean o integration)
   * @param source        source del dataset
   * @param delta_version versión Delta de la capa anterior a recargar
   */
  def delta_version_reload(table: String, layer: String, source: String, delta_version: Int): Unit = {
    logger.info(s"Reloading $table table in $layer with $delta_version delta version from previous layer")

    Try {
      layer.toLowerCase match {
        case CommonsConstants.RAW_CLEAN =>
          val datasetDao: DatasetDao = new DatasetDao
          val dataset = datasetDao.getDatasetByName(table, source)
          logResolvedDataset(source, dataset)

          HistoricalQuality.startHistoricalQuality(HistoricalQualityJobParams(source, Seq(dataset), Some(delta_version)))

        case CommonsConstants.INTEGRATION =>
          val datarelationshipDao: DataRelationshipDao = new DataRelationshipDao
          val distillations = datarelationshipDao.getDataRelationshipByDatasetName(Seq(table), source)
          logResolvedDataRelationships(source, distillations)

          HistoricalIntegration.startHistoricalIntegration(HistoricalIntegrationJobParams(source, distillations, Some(delta_version)))

        case _ => throw new IllegalArgumentException("""Invalid specified layer: to reload a specific delta version from the previous layer the only possibles values are "raw_clean" or "integration" (Nothing was done)""")
      }
    } match {
      case Success(result) =>
        logger.info(s"$result")
        logger.info(s"Delta version reloading of the table $table, in $layer layer, has finished successfully")
      case Failure(e) =>
        logger.error(s"Something went wrong when trying to reload table $table in $layer layer with $delta_version delta version from previous layer", e.getMessage)
        throw new LakehouseDefaultException(s"Error when trying to reload table $table in $layer layer with $delta_version delta version from previous layer:\n ${e.getMessage}", e)
    }
  }

  /**
   * Actualiza el esquema de una tabla de la capa integration recasteándola según
   * su CSV de esquema y sobreescribiendo la tabla con overwriteSchema.
   *
   * @param table  tabla (distillation) a actualizar
   * @param layer  capa (debe ser integration)
   * @param source source de la tabla
   */
  def update_schema_integration(table: String, layer: String, source: String): Unit = {
    logger.info(s"Updating schema for $table table in Integration layer")
    Try {
      layer.toLowerCase match {
        case CommonsConstants.INTEGRATION =>
          val distillationDao: DistillationDao = new DistillationDao
          val distillation: Distillation = distillationDao.getDistillationByName(table)

          logger.info(s"Reading entire ${distillation.datum} table from Integration layer ($source source)")
          DeltaTablesManager.readDeltaTable(layer, source, distillation.datum) match {
            case Right(dfIntegration) =>
              val dfIntegrationNewSchema = castDataFrameByCSVSchema(source, distillation.datum, dfIntegration)
              val dsIntegrationPath = ConstructorPaths.buildSilverIntegrationPath(s"$source", distillation.datum)

              val partitions = Option(distillation.partition_by)
                .map(_.split(",").map(_.trim).toList).getOrElse(List.empty[String])
              val overwriteSchemaOption = Map("overwriteSchema" -> "true")

              logger.info(s"Writing DataFrame with changed schema to 'Integration' layer: ${distillation.datum}")
              val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite,
                partitionBy = partitions, otherOptions = overwriteSchemaOption)
              sparkWriteData(s"${CommonsConstants.INTEGRATION}.$source.${distillation.datum}", dfIntegrationNewSchema, dsIntegrationPath, "saveAsTable", sparkOptions)

            case Left(exception) =>
              throw new LakehouseDefaultException("Error running DeltaTableManager readDeltaTable() function", exception)
          }
        case _ => throw new IllegalArgumentException("""Invalid specified layer: to update the schema of a table in Integration layer, the only possible value is "integration" (Nothing was done)""")
      }
    } match {
      case Success(result) =>
        logger.info(s"Schema update for $table, in $layer layer, has finished successfully")
      case Failure(e) =>
        logger.error(s"Something went wrong when trying to update schema for $table in $layer layer", e.getMessage)
        throw new LakehouseDefaultException(s"Something went wrong when trying to update schema for $table in $layer layer", e)
    }
  }

  /**
   * Elimina una columna de la tabla Delta: activa column mapping, hace el DROP
   * COLUMN y reorganiza la tabla (REORG PURGE).
   *
   * @param table      tabla a modificar
   * @param layer      capa/catálogo de la tabla
   * @param source     source (esquema) de la tabla
   * @param columnName columna a eliminar
   */
  def delete_column(table: String, layer: String, source: String, columnName: String): Unit = {
    logger.info(s"Deleting $columnName column for $table table in $layer layer")
    val properties = List(
      "'delta.minReaderVersion' = '2'",
      "'delta.minWriterVersion' = '5'",
      "'delta.columnMapping.mode' = 'name'"
    )

    val result = for {
      _ <- DeltaTablesManager.setPropertiesDeltaTable(layer, source, table, properties)
      _ <- DeltaTablesManager.dropColumnDeltaTable(layer, source, table, columnName)
      _ <- DeltaTablesManager.reorgDeltaTable(layer, source, table)
    } yield ()

    result match {
      case Right(_) =>
        logger.info(s"$columnName column was deleted for $table table in $layer layer")
      case Left(e) =>
        logger.error(s"Something went wrong when trying to delete $columnName column for $table in $layer layer", e)
        throw new LakehouseDefaultException(s"Error when trying to delete $columnName column for $table in $layer layer\n" +
          s"${e.getMessage}", e)
    }
  }

  /**
   * Añade una columna a la tabla Delta.
   *
   * @param table      tabla a modificar
   * @param layer      capa/catálogo de la tabla
   * @param source     source (esquema) de la tabla
   * @param columnName columna a añadir
   * @param columnType tipo de la nueva columna
   */
  def add_column(table: String, layer: String, source: String, columnName: String, columnType: String): Unit = {
    logger.info(s"Adding $columnName column ($columnType) for $table table in $layer layer")
    DeltaTablesManager.addColumnDeltaTable(layer, source, table, columnName, columnType) match {
      case Right(_) =>
        logger.info(s"$columnName column ($columnType) was added for $table table in $layer layer")
      case Left(e) =>
        logger.error(s"Something went wrong when trying to add $columnName column for $table in $layer layer", e)
        throw new LakehouseDefaultException(s"Error when trying to add $columnName column for $table in $layer layer\n" +
          s"${e.getMessage}", e)
    }
  }

  /**
   * Renombra una columna de la tabla Delta activando antes el column mapping.
   *
   * @param table         tabla a modificar
   * @param layer         capa/catálogo de la tabla
   * @param source        source (esquema) de la tabla
   * @param columnName    columna a renombrar
   * @param newColumnName nuevo nombre de la columna
   */
  def rename_column(table: String, layer: String, source: String, columnName: String, newColumnName: String): Unit = {
    logger.info(s"Renaming $columnName column to $newColumnName for $table table in $layer layer")
    val properties = List(
        "'delta.minReaderVersion' = '2'",
        "'delta.minWriterVersion' = '5'",
        "'delta.columnMapping.mode' = 'name'"
    )
    val result = for {
      _ <- DeltaTablesManager.setPropertiesDeltaTable(layer, source, table, properties)
      _ <- DeltaTablesManager.renameColumnDeltaTable(layer, source, table, columnName, newColumnName)
    } yield ()

    result match {
      case Right(_) =>
        logger.info(s"$columnName column was renamed to $newColumnName for $table table in $layer layer")
      case Left(e) =>
        logger.error(s"Something went wrong when trying to rename $columnName column to $newColumnName for $table in $layer layer", e)
        throw new LakehouseDefaultException(s"Error when trying to rename $columnName column to $newColumnName for $table in $layer layer\n" +
          s"${e.getMessage}", e)
    }
  }

  /**
   * Cambia el tipo de una columna de la tabla Delta reescribiéndola con el cast aplicado.
   *
   * @param table      tabla a modificar
   * @param layer      capa/catálogo de la tabla
   * @param source     source (esquema) de la tabla
   * @param columnName columna a retipar
   * @param columnType tipo destino de la columna
   */
  def cast_column(table: String, layer: String, source: String, columnName: String, columnType: String): Unit = {
    logger.info(s"Casting $columnName column to $columnType for $table table in $layer layer")
    DeltaTablesManager.typeColumnDeltaTable(layer, source, table, columnName, columnType) match {
      case Right(_) =>
        logger.info(s"$columnName column was casted to $columnType for $table table in $layer layer")
      case Left(e) =>
        logger.error(s"Something went wrong when trying to cast $columnName column to $columnType for $table in $layer layer", e)
        throw new LakehouseDefaultException(s"Error when trying to cast $columnName column to $columnType for $table in $layer layer\n" +
          s"${e.getMessage}", e)
    }
  }
}
