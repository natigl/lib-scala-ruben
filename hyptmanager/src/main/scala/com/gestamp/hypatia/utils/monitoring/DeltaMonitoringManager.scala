package com.gestamp.hypatia.utils.monitoring

import org.apache.log4j.Logger
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import java.time.LocalDate
import scala.collection.mutable.{ArrayBuffer, Set}
import scala.util.Try
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DeltaMonitoringManager() {

  val logger: Logger = Logger.getLogger(this.getClass)

  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

  /**
   * Recorre recursivamente el linaje de la tabla (system.access.table_lineage)
   * hacia sus orígenes y devuelve el linaje agrupado por capa (raw_sources,
   * raw_clean, integration, semantic) junto con la última carga de la tabla.
   *
   * @param fullTableName nombre completo de la tabla (catalogo.schema.tabla)
   * @return tupla (tabla, linaje raw_sources, linaje raw_clean, linaje integration,
   *         linaje semantic, timestamp de la última carga)
   */
  def getRecursiveLineageAndLastLoad(fullTableName: String): (String, String, String, String, String, Timestamp) = {
    logger.info(s"[INFO] Starting recursive lineage and last load extraction for table: $fullTableName")
    try {
      val Array(catalog, schema, table) = fullTableName.split('.')
      val visited = Set[String]()
      val toVisit = ArrayBuffer((catalog, schema, table))
      val lineageBuffer = ArrayBuffer[String]()

      while (toVisit.nonEmpty) {
        val (cat, sch, tbl) = toVisit.remove(0)
        val key = s"$cat.$sch.$tbl"
        if (!visited.contains(key)) {
          visited += key

          val query =
            s"""
               |SELECT DISTINCT source_table_catalog, source_table_schema, source_table_name, source_type,
               |       MAX(event_time) OVER (PARTITION BY source_table_catalog, source_table_schema, source_table_name) as last_event_time
               |FROM system.access.table_lineage
               |WHERE target_table_catalog = '$cat'
               |  AND target_table_schema = '$sch'
               |  AND target_table_name = '$tbl'
             """.stripMargin

          val upstreams = SparkConfiguration.spark.sql(query).collect()
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

          upstreams.foreach { row =>
            val srcCat = Option(row.get(0)).map(_.toString).orNull
            val srcSch = Option(row.get(1)).map(_.toString).orNull
            val srcTbl = Option(row.get(2)).map(_.toString).orNull
            val srcType = Option(row.get(3)).getOrElse("UNKNOWN")
            val lastEvt: String = Option(row.get(4))
              .map(_.asInstanceOf[Timestamp].toLocalDateTime().format(formatter))
              .getOrElse("UNKNOWN")

            val srcFull = if (srcCat == null || srcSch == null || srcTbl == null) "EXTERNAL_SOURCE"
            else s"$srcCat.$srcSch.$srcTbl"

            lineageBuffer += s"$srcFull($srcType - $lastEvt)"

            if (srcCat != null && srcSch != null && srcTbl != null)
              toVisit += ((srcCat, srcSch, srcTbl))
          }
        }
      }

      val rawSourcesLineage = lineageBuffer.filter(_.startsWith("raw_sources")).distinct.sorted.mkString(", ")
      val rawCleanLineage = lineageBuffer.filter(_.startsWith("raw_clean")).distinct.sorted.mkString(", ")
      val integrationLineage = lineageBuffer.filter(_.startsWith("integration")).distinct.sorted.mkString(", ")
      val semanticLineage = lineageBuffer.filter(_.startsWith("semantic")).distinct.sorted.mkString(", ")

      val lastLoadQuery =
        s"""
           |SELECT MAX(event_time) as last_load
           |FROM system.access.table_lineage
           |WHERE target_table_catalog = '$catalog'
           |  AND target_table_schema = '$schema'
           |  AND target_table_name = '$table'
         """.stripMargin

      val row = SparkConfiguration.spark.sql(lastLoadQuery).head
      val lastLoadGold: Timestamp = Option(row.get(0)).map(_.asInstanceOf[Timestamp]).orNull

      logger.info(s"[INFO] Lineage and last load extraction completed for $fullTableName")
      (s"$catalog.$schema.$table", rawSourcesLineage, rawCleanLineage, integrationLineage, semanticLineage, lastLoadGold)

    } catch {
      case e: Exception =>
        logger.error(s"[ERROR] Failed to extract lineage and last load for $fullTableName: ${e.getMessage}", e)
        throw e
    }
  }

  /**
   * Compara la fecha de última carga de la tabla gold con las de sus orígenes por
   * capa y clasifica cada origen en actualizado después o antes que la gold,
   * devolviendo además el estado global (updated/delayed).
   *
   * @param lastLoadGold        última carga de la tabla gold
   * @param rawSourcesLineage   linaje de raw_sources con timestamps
   * @param rawCleanLineage     linaje de raw_clean con timestamps
   * @param integrationLineage  linaje de integration con timestamps
   * @param semanticLineage     linaje de semantic con timestamps
   * @return tupla (orígenes actualizados después de gold, orígenes atrasados, estado)
   */
  def computeLoadAlertExpanded(
                                lastLoadGold: Timestamp,
                                rawSourcesLineage: String,
                                rawCleanLineage: String,
                                integrationLineage: String,
                                semanticLineage: String
                              ): (String, String, String) = {

    logger.info(s"[INFO] Computing load alert status...")

    if (lastLoadGold == null) {
      logger.warn(s"[WARN] Last load timestamp is null. Returning empty status.")
      return ("", "", "")
    }

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

    /*

    val lastLoadDateTime = Try(LocalDateTime.parse(lastLoadGold.toString, formatter))
      .getOrElse {
        logger.warn(s"[WARN] Failed to parse last load datetime: $lastLoadGold")
        return ("", "", "")
      }

     */

    val lastLoadDateTime = Try(lastLoadGold.toLocalDateTime)
      .getOrElse {
        logger.warn(s"[WARN] Failed to parse last load datetime: $lastLoadGold")
        return ("", "", "")
      }

    val allLineages = Seq(rawSourcesLineage, rawCleanLineage, integrationLineage, semanticLineage)
    logger.info(s"[INFO] allLineages: $allLineages")

    val datePattern = raw"([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\([A-Z]+ - ([\d\-\s\.:T]+)\)".r

    val statusTuples: Seq[(String, LocalDateTime)] = allLineages.flatMap { lineage =>
      datePattern.findAllMatchIn(lineage).flatMap { m =>
        val table = m.group(1)
        val dateStr = m.group(2)
        Try(LocalDateTime.parse(dateStr, formatter)).toOption.map(dateTime => (table, dateTime))
      }
    }
    logger.info(s"[INFO] statusTuples: $statusTuples")

    val updatedTables = statusTuples.collect {
      case (table, date) if date.isAfter(lastLoadDateTime) => table
    }.distinct.sorted

    logger.info(s"[INFO] updatedTables: $updatedTables")

    val updatedAfterGold = updatedTables.mkString(",")

    val oldTables = statusTuples.collect {
      case (table, date) if !date.toLocalDate.isEqual(lastLoadDateTime.toLocalDate) => table
    }.distinct.sorted

    logger.info(s"[INFO] oldTables: $oldTables")

    val updatedBeforeGold = oldTables.mkString(",")

    val goldStatus = if (updatedAfterGold.isEmpty && updatedBeforeGold.isEmpty) "updated" else "delayed"

    logger.info(s"[INFO] updated_after_gold: $updatedAfterGold")
    logger.info(s"[INFO] updated_before_gold: $updatedBeforeGold")
    logger.info(s"[INFO] gold_status: $goldStatus")

    (updatedAfterGold, updatedBeforeGold, goldStatus)
  }

  /**
   * Calcula la diferencia de filas entre las dos últimas versiones de la tabla Delta.
   *
   * @param table nombre completo de la tabla
   * @return la diferencia como string, o un mensaje si no hay versiones suficientes o falla
   */
  def getRowCountDiff(table: String): String = {
    logger.info(s"[INFO] Computing row count difference for $table")
    try {
      val versions = SparkConfiguration.spark.sql(s"DESCRIBE HISTORY $table")
        .select("version").limit(3).collect().map(_.getLong(0))

      if (versions.length < 2) {
        logger.warn(s"[WARN] Not enough versions found for $table to compute row difference")
        return "Not enough versions"
      }

      val latest = SparkConfiguration.spark.read.format("delta").option("versionAsOf", versions(0)).table(table).count()
      val previous = SparkConfiguration.spark.read.format("delta").option("versionAsOf", versions(1)).table(table).count()

      val diff = latest - previous
      logger.info(s"[INFO] Row count difference for $table: $diff")
      diff.toString
    } catch {
      case e: Exception =>
        logger.error(s"[ERROR] Failed to compute row count diff for $table: ${e.getMessage}", e)
        "ERROR"
    }
  }

  /**
   * Compara el esquema de la última versión de la tabla Delta con el de dos
   * versiones atrás y describe las columnas añadidas, eliminadas o retipadas.
   *
   * @param table nombre completo de la tabla
   * @return descripción de los cambios, o un mensaje si no hay versiones suficientes o falla
   */
  def getSchemaDiff(table: String): String = {
    logger.info(s"[INFO] Checking schema differences for $table")
    try {
      val versions = SparkConfiguration.spark.sql(s"DESCRIBE HISTORY $table")
        .select("version").limit(3).collect().map(_.getLong(0))

      if (versions.length < 3) {
        logger.warn(s"[WARN] Not enough versions found for $table to compare schema")
        return "Not enough versions"
      }

      val schema0 = SparkConfiguration.spark.read.format("delta").option("versionAsOf", versions(0)).table(table).schema
      val schema2 = SparkConfiguration.spark.read.format("delta").option("versionAsOf", versions(2)).table(table).schema

      val fields0 = schema0.map(f => f.name -> f.dataType.simpleString).toMap
      val fields2 = schema2.map(f => f.name -> f.dataType.simpleString).toMap

      val added = fields0.keySet.diff(fields2.keySet)
      val removed = fields2.keySet.diff(fields0.keySet)
      val changed = fields0.keySet.intersect(fields2.keySet).filter(f => fields0(f) != fields2(f))

      val parts = ArrayBuffer[String]()
      if (added.nonEmpty) parts += s"Added: ${added.mkString(", ")}"
      if (removed.nonEmpty) parts += s"Removed: ${removed.mkString(", ")}"
      if (changed.nonEmpty) parts += s"Changed: ${changed.map(f => s"$f (${fields2(f)} -> ${fields0(f)})").mkString(", ")}"

      val result = if (parts.isEmpty) "No changes" else parts.mkString(" | ")
      logger.info(s"[INFO] Schema differences for $table: $result")
      result
    } catch {
      case e: Exception =>
        logger.error(s"[ERROR] Failed to compute schema diff for $table: ${e.getMessage}", e)
        "ERROR"
    }
  }
}
