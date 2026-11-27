package com.gestamp.hypatia.utils.tag_propagation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Trait for accessing system tables.
 * Allows for testing by providing alternative implementations.
 */
trait SystemTableAccessor {
  /** Devuelve los tags (tag_name, tag_value) de una columna concreta. */
  def getColumnTags(table: TableRef, columnName: String): DataFrame
  /** Devuelve los tags de todas las columnas de una tabla. */
  def getAllColumnTags(table: TableRef): DataFrame
  /** Devuelve las columnas de una tabla en orden ordinal. */
  def getTableColumns(table: TableRef): DataFrame
  /** Devuelve el linaje directo (tablas/columnas origen) de una columna en los últimos daysBack días. */
  def getColumnLineage(targetTable: TableRef, columnName: String, daysBack: Int): DataFrame
  /** Devuelve el linaje recursivo de una columna hasta maxDepth niveles, filtrado por patrón de capa. */
  def getRecursiveColumnLineage(targetTable: TableRef, columnName: String,
                                 targetLayerPattern: String, maxDepth: Int, daysBack: Int): DataFrame
}

/**
 * Default implementation that queries actual Databricks system tables.
 *
 * @param useLineageApi true para consultar primero la API REST de linaje (tabla recién creada, cuyo linaje aún no está
 *                      en system.access.column_lineage); false para ir directamente a la tabla de sistema (tabla que ya
 *                      existía y tiene linaje de ejecuciones anteriores)
 */
class DatabricksSystemTableAccessor(spark: SparkSession,
                                    useLineageApi: Boolean = true,
                                    lineageApi: => LineageApiClient = LineageApiClient.fromDbutilsContext(),
                                    lineageWaitAttempts: Int = 6,
                                    lineageWaitSleepMs: Long = 10000L) extends SystemTableAccessor {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val api: LineageApiClient = lineageApi

  /** Tablas para las que ya se esperó al linaje en la API (una espera por tabla, no por columna). */
  private val waitedTables = scala.collection.mutable.Map[String, Boolean]()

  /**
   * Consulta los tags de una columna en system.information_schema.column_tags.
   *
   * @param catalog    catálogo de la tabla
   * @param schema     esquema de la tabla
   * @param tableName  nombre de la tabla
   * @param columnName nombre de la columna
   * @return DataFrame con (tag_name, tag_value)
   */
  override def getColumnTags(table: TableRef, columnName: String): DataFrame = {
    val query = s"""
      SELECT tag_name, tag_value
      FROM system.information_schema.column_tags
      WHERE catalog_name = '${table.catalog}'
        AND schema_name = '${table.schema}'
        AND table_name = '${table.table}'
        AND column_name = '$columnName'
    """
    logger.info(s"Executing getColumnTags query: $query")
    spark.sql(query)
  }

  /**
   * Consulta los tags de todas las columnas de la tabla en system.information_schema.column_tags.
   *
   * @param catalog   catálogo de la tabla
   * @param schema    esquema de la tabla
   * @param tableName nombre de la tabla
   * @return DataFrame con (column_name, tag_name, tag_value)
   */
  override def getAllColumnTags(table: TableRef): DataFrame = {
    val query = s"""
      SELECT column_name, tag_name, tag_value
      FROM system.information_schema.column_tags
      WHERE catalog_name = '${table.catalog}'
        AND schema_name = '${table.schema}'
        AND table_name = '${table.table}'
    """
    logger.info(s"Executing getAllColumnTags query: $query")
    spark.sql(query)
  }

  /**
   * Consulta las columnas de la tabla en system.information_schema.columns,
   * ordenadas por posición.
   *
   * @param catalog   catálogo de la tabla
   * @param schema    esquema de la tabla
   * @param tableName nombre de la tabla
   * @return DataFrame con column_name
   */
  override def getTableColumns(table: TableRef): DataFrame = {
    val query = s"""
      SELECT column_name
      FROM system.information_schema.columns
      WHERE table_catalog = '${table.catalog}'
        AND table_schema = '${table.schema}'
        AND table_name = '${table.table}'
      ORDER BY ordinal_position
    """
    logger.info(s"Executing getTableColumns query: $query")
    spark.sql(query)
  }

  /**
   * Linaje directo de la columna: tablas y columnas origen.
   *
   * Si useLineageApi, primero consulta la API REST de linaje (LineageApiClient), que refleja una escritura recién hecha en segundos o
   * minutos; la primera vez por tabla espera hasta lineageWaitAttempts x lineageWaitSleepMs a que aparezca su linaje.
   * Si la API no se puede usar, falla o no devuelve fuentes, consulta system.access.column_lineage (últimos daysBack
   * días), que llega con horas de retraso: sin este primer paso una tabla Gold nueva se quedaba sin tags (y sin row
   * filter) hasta una ejecución posterior.
   *
   * @param fullTableName nombre completo de la tabla destino (catalogo.schema.tabla)
   * @param columnName    columna destino
   * @param daysBack      ventana de días hacia atrás a considerar (solo para la tabla de sistema)
   * @return DataFrame con (source_table_catalog, source_table_schema, source_table_name, source_column_name)
   */
  override def getColumnLineage(targetTable: TableRef, columnName: String, daysBack: Int): DataFrame = {
    val fullTableName = targetTable.full
    lineageFromApi(fullTableName, columnName) match {
      case Right(sources) =>
        consoleLog(s"[Lineage] $fullTableName.$columnName — source: REST API, ${sources.size} source column(s): " +
          sources.map(s => s"${s.catalog}.${s.schema}.${s.tableName}.${s.columnName}").mkString(", "))
        import spark.implicits._
        sources.map(s => (s.catalog, s.schema, s.tableName, s.columnName))
          .toDF("source_table_catalog", "source_table_schema", "source_table_name", "source_column_name")
      case Left(reason) =>
        consoleLog(s"[Lineage] $fullTableName.$columnName — source: system.access.column_lineage ($reason)")
        lineageFromSystemTable(fullTableName, columnName, daysBack)
    }
  }

  // Log y stdout: en el job JAR de USAFederated el log de la aplicación llega incompleto a la salida del run.
  private def consoleLog(msg: String): Unit = { logger.info(msg); println(msg) }

  // Right(fuentes) si la API devuelve fuentes; Left(motivo) para ir a la tabla de sistema.
  private def lineageFromApi(fullTableName: String, columnName: String): Either[String, Seq[LineageSourceColumn]] = {
    if (!useLineageApi) return Left("existing table, REST API not used")
    if (!api.available) return Left("REST API not available: no host/token in the dbutils context")
    val ready = waitedTables.getOrElseUpdate(fullTableName,
      api.waitForTableLineage(fullTableName, lineageWaitAttempts, lineageWaitSleepMs))
    if (!ready) return Left("REST API has no table lineage yet or failed, fallback")
    api.columnUpstreams(fullTableName, columnName) match {
      case Some(sources) if sources.nonEmpty => Right(sources)
      case Some(_) => Left("REST API returned no source for this column, fallback")
      case None    => Left("REST API column lineage call failed, fallback")
    }
  }

  private def lineageFromSystemTable(fullTableName: String, columnName: String, daysBack: Int): DataFrame = {
    val query = s"""
      SELECT DISTINCT
        source_table_catalog,
        source_table_schema,
        source_table_name,
        source_column_name
      FROM system.access.column_lineage
      WHERE target_table_full_name = '$fullTableName'
        AND target_column_name = '$columnName'
        AND event_date >= current_date() - INTERVAL $daysBack DAYS
        AND source_table_full_name IS NOT NULL
        AND source_column_name IS NOT NULL
    """
    logger.info(s"Executing getColumnLineage query for $fullTableName.$columnName: $query")
    spark.sql(query)
  }

  /**
   * Consulta el linaje recursivo de la columna con una CTE recursiva sobre
   * system.access.column_lineage, evitando ciclos y limitando la profundidad,
   * y filtra los orígenes cuya capa (catálogo/esquema/tabla) case con el patrón.
   *
   * @param fullTableName      nombre completo de la tabla destino
   * @param columnName         columna destino
   * @param targetLayerPattern patrón (LIKE) de la capa origen buscada
   * @param maxDepth           profundidad máxima de la recursión
   * @param daysBack           ventana de días hacia atrás a considerar
   * @return DataFrame con los orígenes encontrados y su profundidad
   */
  override def getRecursiveColumnLineage(targetTable: TableRef, columnName: String,
                                          targetLayerPattern: String, maxDepth: Int, daysBack: Int): DataFrame = {
    val fullTableName = targetTable.full
    val query = s"""
      WITH RECURSIVE column_lineage_tree AS (
        -- Base: linaje directo de la columna gold
        SELECT
          source_table_catalog,
          source_table_schema,
          source_table_name,
          source_column_name,
          source_table_full_name,
          1 AS depth,
          ARRAY(CONCAT(source_table_full_name, '.', source_column_name)) AS path
        FROM system.access.column_lineage
        WHERE target_table_full_name = '$fullTableName'
          AND target_column_name = '$columnName'
          AND event_date >= current_date() - INTERVAL $daysBack DAYS
          AND source_table_full_name IS NOT NULL
          AND source_column_name IS NOT NULL

        UNION ALL

        -- Recursivo: linaje de las fuentes
        SELECT
          cl.source_table_catalog,
          cl.source_table_schema,
          cl.source_table_name,
          cl.source_column_name,
          cl.source_table_full_name,
          clt.depth + 1 AS depth,
          ARRAY_APPEND(clt.path, CONCAT(cl.source_table_full_name, '.', cl.source_column_name)) AS path
        FROM system.access.column_lineage cl
        INNER JOIN column_lineage_tree clt
          ON cl.target_table_full_name = clt.source_table_full_name
          AND cl.target_column_name = clt.source_column_name
        WHERE clt.depth < $maxDepth
          AND cl.event_date >= current_date() - INTERVAL $daysBack DAYS
          AND cl.source_table_full_name IS NOT NULL
          AND cl.source_column_name IS NOT NULL
          AND NOT ARRAY_CONTAINS(clt.path, CONCAT(cl.source_table_full_name, '.', cl.source_column_name))
      )
      SELECT DISTINCT
        source_table_catalog,
        source_table_schema,
        source_table_name,
        source_column_name,
        depth
      FROM column_lineage_tree
      WHERE LOWER(source_table_catalog) LIKE '%${targetLayerPattern.toLowerCase}%'
         OR LOWER(source_table_schema) LIKE '%${targetLayerPattern.toLowerCase}%'
         OR LOWER(source_table_name) LIKE '%${targetLayerPattern.toLowerCase}%'
      ORDER BY depth
    """
    logger.info(s"Executing getRecursiveColumnLineage query for $fullTableName.$columnName (pattern: $targetLayerPattern, maxDepth: $maxDepth): $query")
    spark.sql(query)
  }
}
