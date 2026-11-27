package com.gestamp.hypatia.utils.monitoring

import org.slf4j.{Logger, LoggerFactory}
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp

/**
 * Orquestador de la monitorización Delta: recorre las tablas de un catálogo.esquema
 * y calcula para cada una su linaje, alertas de carga y diferencias de filas/esquema.
 *
 * @param manager manager con las operaciones de monitorización por tabla
 */
class DeltaMonitoringOrchestrator(manager: DeltaMonitoringManager) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Ejecuta la monitorización de todas las tablas MANAGED/EXTERNAL del
   * catálogo.esquema: linaje recursivo, estado de carga respecto a gold,
   * diferencia de filas y de esquema. Las tablas que fallan se omiten.
   *
   * @param catalog catálogo a monitorizar
   * @param schema  esquema a monitorizar
   * @return los resultados de monitorización de las tablas procesadas
   */
  def execute(catalog: String, schema: String): Seq[TableMonitoringResult] = {
    val now = LocalDateTime.now()
    val nowTs = Timestamp.valueOf(now)

    logger.info(s"Starting execution of Delta Monitoring for catalog.schema: $catalog.$schema")

    logger.info(s"Fetching tables from catalog.schema: $catalog.$schema")
    val tables = SparkConfiguration.spark.catalog.listTables(s"$catalog.$schema")
      .filter(t => t.tableType == "MANAGED" || t.tableType == "EXTERNAL")
      .collect()
    logger.info(s"Found ${tables.length} tables to monitor")

    val results = tables.flatMap { t =>
      val fullTable = s"$catalog.$schema.${t.name}"
      logger.info(s"Starting processing of table: $fullTable")
      try {
        val (name, rawSourcesLineage, rawCleanLineage, integrationLineage, semanticLineage, lastLoadGold) =
          manager.getRecursiveLineageAndLastLoad(fullTable)
        logger.info(s"Lineage and last load retrieved for $fullTable")

        val (updatedAfterGold, updatedBeforeGold, goldStatus) =
          manager.computeLoadAlertExpanded(lastLoadGold, rawSourcesLineage, rawCleanLineage, integrationLineage, semanticLineage)
        logger.info(s"Load alert of tables updated after gold table for $fullTable: $updatedAfterGold")
        logger.info(s"Load alert of tables updated before(more than 1 day) gold table for $fullTable: $updatedBeforeGold")

        val rowCountDiff = manager.getRowCountDiff(fullTable)
        logger.info(s"Row count difference computed for $fullTable: $rowCountDiff")

        val schemaDiff = manager.getSchemaDiff(fullTable)
        logger.info(s"Schema difference computed for $fullTable: $schemaDiff")

        logger.info(s"Completed processing of table: $fullTable")

        Some(TableMonitoringResult(
          table_name = name,
          raw_sources_lineage = rawSourcesLineage,
          raw_clean_lineage = rawCleanLineage,
          integration_lineage = integrationLineage,
          semantic_lineage = semanticLineage,
          last_load_gold = lastLoadGold,
          updated_after_gold = updatedAfterGold,
          updated_before_gold = updatedBeforeGold,
          gold_status = goldStatus,
          row_count_diff = rowCountDiff,
          schema_diff = schemaDiff,
          monitoring_date = nowTs
        ))
      } catch {
        case e: Exception =>
          logger.error(s"[ERROR] Failed to process table $fullTable: ${e.getMessage}", e)
          None
      }
    }

    logger.info(s"Execution completed for all tables in catalog.schema: $catalog.$schema. Processed ${results.length} tables successfully.")

    results
  }
}
