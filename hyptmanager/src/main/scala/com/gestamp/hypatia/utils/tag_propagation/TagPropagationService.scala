package com.gestamp.hypatia.utils.tag_propagation

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}
import java.sql.Timestamp

class TagPropagationService(spark: SparkSession, accessor: SystemTableAccessor = null) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val tableAccessor: SystemTableAccessor =
    if (accessor != null) accessor else new DatabricksSystemTableAccessor(spark)

  private val governedTagPriority: Seq[String] = TagPropagationService.GovernedTagPriority
  private val governedTagKeys: Set[String] = TagPropagationService.GovernedTagKeys

  /**
   * Obtiene las etiquetas de una columna específica.
   */
  def getColumnTags(catalog: String, schema: String, tableName: String, columnName: String): Seq[Tag] = {
    try {
      val df = tableAccessor.getColumnTags(TableRef(catalog, schema, tableName), columnName)

      df.collect().map(row => Tag(
        key = row.getAs[String]("tag_name"),
        value = row.getAs[String]("tag_value")
      )).toSeq
    } catch {
      case e: Exception =>
        logger.warn(s"Could not get tags for $catalog.$schema.$tableName.$columnName: ${e.getMessage}")
        Seq.empty
    }
  }

  /**
   * Obtiene todas las etiquetas de columnas para una tabla.
   */
  def getAllColumnTags(catalog: String, schema: String, tableName: String): Map[String, Seq[Tag]] =
    safeReadAllColumnTags(TableRef(catalog, schema, tableName))

  // Lee los column tags capturando errores (devuelve vacío si falla), para el camino aditivo no estricto.
  private def safeReadAllColumnTags(table: TableRef): Map[String, Seq[Tag]] = {
    try {
      readAllColumnTags(table)
    } catch {
      case e: Exception =>
        logger.warn(s"Could not get column tags for ${table.full}: ${e.getMessage}")
        Map.empty
    }
  }

  // Lee los column tags de una tabla agrupados por columna, sin capturar errores.
  private def readAllColumnTags(table: TableRef): Map[String, Seq[Tag]] = {
    tableAccessor.getAllColumnTags(table)
      .collect()
      .groupBy(_.getAs[String]("column_name"))
      .map { case (colName, rows) =>
        colName -> rows.map(r => Tag(r.getAs[String]("tag_name"), r.getAs[String]("tag_value"))).toSeq
      }
  }

  /**
   * Obtiene las columnas de una tabla.
   */
  def getTableColumns(catalog: String, schema: String, tableName: String): Seq[String] = {
    try {
      readTableColumns(TableRef(catalog, schema, tableName))
    } catch {
      case e: Exception =>
        logger.warn(s"Could not get columns for $catalog.$schema.$tableName: ${e.getMessage}")
        Seq.empty
    }
  }

  // Lee las columnas de una tabla en orden ordinal, sin capturar errores.
  private def readTableColumns(table: TableRef): Seq[String] =
    tableAccessor.getTableColumns(table)
      .collect().map(_.getAs[String]("column_name")).toSeq

  /**
   * Obtiene el linaje upstream de una columna hasta una capa específica (ej: silver).
   * Filtra solo las fuentes que pertenecen a la capa objetivo.
   */
  def getColumnLineageToLayer(
                               goldCatalog: String,
                               goldSchema: String,
                               goldTable: String,
                               goldColumn: String,
                               targetLayer: String = "semantic",  // Patrón para identificar la capa (puede estar en catalog o schema)
                               daysBack: Int = 30
                             ): Seq[ColumnInfo] = {

    // Obtener linaje upstream directo
    val df = tableAccessor.getColumnLineage(TableRef(goldCatalog, goldSchema, goldTable), goldColumn, daysBack)

    val allSources = df.collect().map { row =>
      ColumnInfo(
        catalog = row.getAs[String]("source_table_catalog"),
        schema = row.getAs[String]("source_table_schema"),
        tableName = row.getAs[String]("source_table_name"),
        columnName = row.getAs[String]("source_column_name")
      )
    }.toSeq

    // print(allSources)
    // Filtrar solo las fuentes de la capa silver (sin distinguir mayúsculas: la API REST puede devolver otra caja)
    val silverSources = allSources.filter { col =>
      TableRef.normalize(col.catalog).startsWith(TableRef.normalize(targetLayer))
    }

    // Enriquecer con tags
    silverSources.map { col =>
      val tags = getColumnTags(col.catalog, col.schema, col.tableName, col.columnName)
      col.copy(tags = tags)
    }
  }

  /**
   * Obtiene el linaje recursivo hasta la capa silver usando CTEs recursivos.
   */
  def getRecursiveColumnLineageToLayer(
                                        goldCatalog: String,
                                        goldSchema: String,
                                        goldTable: String,
                                        goldColumn: String,
                                        targetLayerPattern: String = "silver",
                                        maxDepth: Int = 5,
                                        daysBack: Int = 30
                                      ): Seq[ColumnInfo] = {

    val df = tableAccessor.getRecursiveColumnLineage(
      TableRef(goldCatalog, goldSchema, goldTable), goldColumn, targetLayerPattern, maxDepth, daysBack
    )

    df.collect().map { row =>
      val col = ColumnInfo(
        catalog = row.getAs[String]("source_table_catalog"),
        schema = row.getAs[String]("source_table_schema"),
        tableName = row.getAs[String]("source_table_name"),
        columnName = row.getAs[String]("source_column_name")
      )
      val tags = getColumnTags(col.catalog, col.schema, col.tableName, col.columnName)
      col.copy(tags = tags)
    }.toSeq
  }

  /**
   * Obtiene el plan de propagación de tags para todas las columnas de una tabla Gold.
   *
   * Optimización de queries:
   *   Fase 1 — linaje: una query por columna Gold (inevitable, necesitamos saber qué Silver alimenta a qué Gold)
   *   Fase 2 — tags:   una query getAllColumnTags por tabla Silver única (no una por columna Silver)
   *   Fase 3 — plan:   enriquecimiento desde el cache en memoria, sin más queries
   */
  def getTagPropagationPlan(
                             goldCatalog: String,
                             goldSchema: String,
                             goldTable: String,
                             silverLayer: String = "semantic",
                             daysBack: Int = 30
                           ): Seq[TagPropagationPlan] = {

    val goldColumns      = getTableColumns(goldCatalog, goldSchema, goldTable)
    val existingGoldTags = getAllColumnTags(goldCatalog, goldSchema, goldTable)

    buildTagPropagationPlan(goldCatalog, goldSchema, goldTable, silverLayer, daysBack,
      goldColumns, existingGoldTags, strict = false).plans
  }

  /**
   * Obtiene el plan de propagación DESEADO de una tabla Gold: el que se obtendría si la tabla Gold no tuviera
   * ningún tag previo (el mismo resultado que DROP + create + propagación). Aplica las mismas reglas que
   * [[getTagPropagationPlan]] (governed tags, prioridad, multi-fuente y blocked), pero sin descartar las keys
   * que ya tiene Gold y evaluando `goldAlreadyHasGoverned` sobre un estado vacío.
   *
   * Es estricto: si falla la lectura de columnas, linaje o tags de Silver lanza la excepción en vez de
   * devolver vacío, porque un plan deseado incompleto llevaría a quitar tags (y filtro) que sí aplican.
   */
  def getDesiredTagPropagationPlan(
                                    goldCatalog: String,
                                    goldSchema: String,
                                    goldTable: String,
                                    silverLayer: String = "semantic",
                                    daysBack: Int = 30
                                  ): Seq[TagPropagationPlan] =
    desiredPlanWithLineage(goldCatalog, goldSchema, goldTable, silverLayer, daysBack).plans

  // Plan deseado junto con el resumen del linaje Silver encontrado.
  private def desiredPlanWithLineage(goldCatalog: String, goldSchema: String, goldTable: String,
                                     silverLayer: String, daysBack: Int): PlanWithLineage = {

    val goldColumns = readTableColumns(TableRef(goldCatalog, goldSchema, goldTable))

    buildTagPropagationPlan(goldCatalog, goldSchema, goldTable, silverLayer, daysBack,
      goldColumns, Map.empty, strict = true)
  }

  /**
   * Sincroniza los column tags de la tabla Gold con el estado deseado ([[getDesiredTagPropagationPlan]]),
   * sin borrar la tabla. Orden: PRIMERO los SET de los tags deseados que faltan o cambian de valor y, solo si
   * todos han ido bien, DESPUÉS los UNSET de los tags actuales que no están en el deseado. Así, durante la
   * transición la tabla tiene como mucho filtro de más, nunca de menos. Las columnas que ya no existen en la
   * tabla se ignoran. Los table tags no se tocan: la propagación solo gestiona column tags.
   *
   * Guarda de linaje vacío (solo si la tabla no está excluida): no se hace UNSET de ningún tag de una columna Gold
   * cuyo linaje no tenga ninguna fuente Silver (guarda por columna, `columnsWithLineage` de
   * [[TagSyncDiff.compute]]); si ninguna columna tiene fuentes, no se hace ningún UNSET ([[TagSyncDiff.unsetAllowed]]).
   *
   * @param excluded true si la tabla está en la lista de exclusión: el estado deseado es "sin tags"
   * @param dryRun   true para solo loguear las sentencias
   * @return (sql, éxito, mensaje) por sentencia ejecutada
   * @throws Exception si no se puede calcular el estado deseado o leer el actual (no se ha ejecutado nada)
   */
  def syncTagsToDesiredState(
                              goldCatalog: String,
                              goldSchema: String,
                              goldTable: String,
                              silverLayer: String = "semantic",
                              daysBack: Int = 30,
                              excluded: Boolean = false,
                              dryRun: Boolean = true
                            ): Seq[(String, Boolean, String)] = {

    val goldRef = TableRef(goldCatalog, goldSchema, goldTable)
    val fullGoldTable = goldRef.full

    val desired: PlanWithLineage =
      if (excluded) PlanWithLineage(Seq.empty, 0, Set.empty)
      else desiredPlanWithLineage(goldCatalog, goldSchema, goldTable, silverLayer, daysBack)

    val tableColumns = readTableColumns(goldRef)

    val currentTags: Map[String, Seq[Tag]] = readAllColumnTags(goldRef)

    val unsetAllowed = TagSyncDiff.unsetAllowed(excluded, desired.lineageSourceCount)

    if (!unsetAllowed && currentTags.values.exists(_.nonEmpty))
      syncLogWarn(s"[TagSync] $fullGoldTable — lineage returned no Silver source for any column, current column " +
        s"tags are kept (no UNSET). Possible latency of system.access.column_lineage or no execution in the last " +
        s"$daysBack days")

    val columnsWithLineage = if (excluded) None else Some(desired.columnsWithLineage)

    val diff = TagSyncDiff.compute(desired.plans, currentTags, tableColumns, unsetAllowed, columnsWithLineage,
      governedKeys = governedTagKeys, tableLevelKeys = Set(TagPropagationService.BlockedTagKey))

    diff.keptByMissingLineage.groupBy(_.columnName).toSeq.sortBy(_._1).foreach { case (col, changes) =>
      syncLogWarn(s"[TagSync] $fullGoldTable.$col — lineage returned no Silver source for this column, its tags " +
        s"${changes.map(c => s"${c.tag.key}=${c.tag.value}").mkString(", ")} are kept (no UNSET). Possible latency " +
        s"of system.access.column_lineage or no execution in the last $daysBack days")
    }

    syncLogInfo(s"[TagSync] $fullGoldTable — ${diff.toSet.size} tag(s) to set, ${diff.toUnset.size} tag(s) to unset " +
      s"(excluded=$excluded, dryRun=$dryRun)")

    val setResults = diff.toSet.map(c => runSql(TagSyncDiff.setTagSQL(fullGoldTable, c), dryRun))

    if (setResults.exists(!_._2)) {
      syncLogError(s"[TagSync] $fullGoldTable — ${setResults.count(!_._2)} SET failed, skipping the " +
        s"${diff.toUnset.size} UNSET statement(s): obsolete tags are kept")
      setResults
    } else {
      setResults ++ diff.toUnset.map(c => runSql(TagSyncDiff.unsetTagSQL(fullGoldTable, c), dryRun))
    }
  }

  // [TagSync] a log y stdout: en el job JAR de USAFederated el log de la aplicación llega incompleto a la salida del run.
  private def syncLogInfo(msg: String): Unit = { logger.info(msg); println(msg) }
  private def syncLogWarn(msg: String): Unit = { logger.warn(msg); println(msg) }
  private def syncLogError(msg: String): Unit = { logger.error(msg); println(msg) }

  // Ejecuta (o loguea en dry run) una sentencia SQL y devuelve (sql, éxito, mensaje).
  private def runSql(sql: String, dryRun: Boolean): (String, Boolean, String) = {
    if (dryRun) {
      logger.info(s"[DRY RUN] $sql")
      (sql, true, "Dry run - not executed")
    } else {
      try {
        spark.sql(sql)
        logger.info(s"[SUCCESS] $sql")
        (sql, true, "Executed successfully")
      } catch {
        case e: Exception =>
          logger.error(s"[ERROR] $sql — ${e.getMessage}")
          (sql, false, e.getMessage)
      }
    }
  }

  /**
   * Construye el plan de propagación a partir de las columnas Gold y sus tags actuales, y devuelve también el
   * número total de fuentes Silver encontradas en el linaje y las columnas Gold que tienen alguna.
   * Con strict = true los fallos de linaje y de lectura de tags Silver se propagan en vez de tratarse como vacío.
   */
  private def buildTagPropagationPlan(
                                       goldCatalog: String,
                                       goldSchema: String,
                                       goldTable: String,
                                       silverLayer: String,
                                       daysBack: Int,
                                       goldColumns: Seq[String],
                                       existingGoldTags: Map[String, Seq[Tag]],
                                       strict: Boolean
                                     ): PlanWithLineage = {

    val goldRef          = TableRef(goldCatalog, goldSchema, goldTable)
    val fullGoldTable    = goldRef.full

    logger.info(s"[TagPropagation] Starting plan for $fullGoldTable — ${goldColumns.size} columns, silverLayer=$silverLayer, daysBack=$daysBack")

    // Fase 1: linaje sin tags — una query por columna Gold
    val lineageByColumn: Map[String, Seq[ColumnInfo]] = goldColumns.map { colName =>
      colName -> getRawSilverSources(goldRef, colName, silverLayer, daysBack, strict)
    }.toMap

    val totalSources = lineageByColumn.values.map(_.size).sum
    logger.info(s"[TagPropagation] Lineage loaded — $totalSources Silver sources found across ${goldColumns.size} Gold columns")

    // Fase 2: una sola llamada getAllColumnTags por tabla Silver única. Las fuentes del linaje pueden venir con
    // otra caja (p. ej. de la API REST); TableRef normaliza catálogo/esquema/tabla para que la clave de caché case
    // con lo que devuelve getAllColumnTags, y el nombre de columna se normaliza aparte (UC lo compara sin caja).
    val uniqueSilverTables: Set[TableRef] =
      lineageByColumn.values.flatten.map(c => TableRef(c.catalog, c.schema, c.tableName)).toSet

    logger.info(s"[TagPropagation] Loading tags from ${uniqueSilverTables.size} unique Silver table(s): ${uniqueSilverTables.map(_.full).mkString(", ")}")

    val silverTagCache: Map[(TableRef, String), Seq[Tag]] =
      uniqueSilverTables.flatMap { ref =>
        val silverTags =
          if (strict) readAllColumnTags(ref)
          else safeReadAllColumnTags(ref)
        silverTags.map { case (colName, tags) =>
          (ref, TableRef.normalize(colName)) -> tags
        }
      }.toMap

    // Fase 3: enriquecer desde cache y construir planes
    val plans = goldColumns.flatMap { colName =>
      val silverSources = lineageByColumn(colName).map { col =>
        col.copy(tags = silverTagCache.getOrElse(
          (TableRef(col.catalog, col.schema, col.tableName), TableRef.normalize(col.columnName)), Seq.empty))
      }
      val goldColumn = ColumnInfo(goldRef.catalog, goldRef.schema, goldRef.table, colName,
        existingGoldTags.getOrElse(colName, Seq.empty))

      if (silverSources.nonEmpty) {
        val allSourceTags   = silverSources.flatMap(_.tags).distinct
        val existingTagKeys = goldColumn.tags.map(_.key).toSet
        val newTags         = allSourceTags.filterNot(t => existingTagKeys.contains(t.key))

        if (newTags.nonEmpty || silverSources.exists(_.tags.nonEmpty)) {
          if (newTags.nonEmpty)
            logger.info(s"[TagPropagation] $fullGoldTable.$colName — will propagate: ${newTags.map(t => s"${t.key}=${t.value}").mkString(", ")}")
          Some(TagPropagationPlan(targetColumn = goldColumn, tagsToPropagate = newTags, sourceColumns = silverSources))
        } else
          None
      } else {
        None
      }
    }

    logger.info(s"[TagPropagation] Plan built — ${plans.size} column(s) with tags to process")

    PlanWithLineage(
      plans = resolveGovernedTags(plans, goldCatalog, goldSchema, goldTable, goldColumns, existingGoldTags),
      lineageSourceCount = totalSources,
      columnsWithLineage = lineageByColumn.collect { case (col, sources) if sources.nonEmpty => col }.toSet)
  }

  // Devuelve las fuentes Silver de una columna Gold sin enriquecer con tags (solo linaje).
  // Con strict = true un fallo de linaje se propaga en vez de devolver vacío.
  private def getRawSilverSources(
    goldRef: TableRef,
    goldColumn: String,
    silverLayer: String,
    daysBack: Int,
    strict: Boolean
  ): Seq[ColumnInfo] = {
    try {
      tableAccessor.getColumnLineage(goldRef, goldColumn, daysBack)
        .collect()
        .map(row => ColumnInfo(
          catalog    = row.getAs[String]("source_table_catalog"),
          schema     = row.getAs[String]("source_table_schema"),
          tableName  = row.getAs[String]("source_table_name"),
          columnName = row.getAs[String]("source_column_name")
        ))
        .filter(col => TableRef.normalize(col.catalog).startsWith(TableRef.normalize(silverLayer)))
        .toSeq
    } catch {
      case e: Exception if !strict =>
        logger.warn(s"Could not get lineage for ${goldRef.full}.$goldColumn: ${e.getMessage}")
        Seq.empty
    }
  }

  /**
   * Gestiona los governed tags (management_company / logistic_center) en un único paso:
   *
   * 1. Columnas multi-fuente: conservan el governed tag si hay uno solo entre sus fuentes y, o bien todas
   *    las fuentes lo tienen, o bien todas son el mismo campo (mismo nombre de columna, p.ej. un join o
   *    union de dos tablas) aunque alguna no esté etiquetada. Si los tags difieren, o se mezclan campos
   *    distintos con y sin tag, es un campo combinado y se eliminan sus governed tags — no son fiables.
   * 2. Entre las columnas candidatas: management_company gana sobre logistic_center (una sola
   *    ocurrencia por tabla).
   * 3. Si Silver tenía governed tags pero Gold no va a tener ninguno tras los pasos anteriores,
   *    se añade Tag("blocked", governed_tag_perdido, a la primera columna Gold.
   */
  private def resolveGovernedTags(
    plans: Seq[TagPropagationPlan],
    goldCatalog: String,
    goldSchema: String,
    goldTable: String,
    goldColumns: Seq[String],
    existingGoldTags: Map[String, Seq[Tag]]
  ): Seq[TagPropagationPlan] = {

    val silverGovernedTagKeys = plans
      .flatMap(_.sourceColumns.flatMap(_.tags))
      .filter(t => governedTagKeys.contains(t.key))
      .map(_.key)
      .toSet

    // Las fuentes aportan un único governed tag y no se mezclan campos distintos con y sin tag
    def sourcesAgreeOnGovernedTag(p: TagPropagationPlan): Boolean = {
      val perSource = p.sourceColumns.map(_.tags.filter(t => governedTagKeys.contains(t.key)).toSet)
      val allTagged = perSource.forall(_.nonEmpty)
      val sameField = p.sourceColumns.map(_.columnName.toLowerCase).distinct.size == 1
      perSource.flatten.toSet.size == 1 && (allTagged || sameField)
    }

    // Paso 1: quitar governed tags de columnas multi-fuente salvo que sus fuentes coincidan
    val cleanedPlans = plans.map { p =>
      if (p.sourceColumns.size > 1 && !sourcesAgreeOnGovernedTag(p))
        p.copy(tagsToPropagate = p.tagsToPropagate.filterNot(t => governedTagKeys.contains(t.key)))
      else
        p
    }

    // Paso 2: prioridad management_company > logistic_center entre las columnas candidatas
    val winnerKey: Option[String] = governedTagPriority.find { key =>
      cleanedPlans.exists(_.tagsToPropagate.exists(_.key == key))
    }

    val multiSourceRemoved = plans.count(p => p.sourceColumns.size > 1 && !sourcesAgreeOnGovernedTag(p) &&
      p.sourceColumns.flatMap(_.tags).exists(t => governedTagKeys.contains(t.key)))
    if (multiSourceRemoved > 0)
      logger.warn(s"[TagPropagation] Governed tags removed from $multiSourceRemoved multi-source column(s) (calculated fields)")

    winnerKey.foreach { key =>
      val distinctValues = cleanedPlans.flatMap(_.tagsToPropagate.filter(_.key == key)).map(_.value).distinct
      if (distinctValues.size > 1) {
        val tableName = plans.headOption.map(_.targetColumn.fullTableName).getOrElse("unknown table")
        logger.warn(s"Conflicting values for governed tag '$key' in $tableName: ${distinctValues.mkString(", ")}. Keeping the first one found.")
      }
      logger.info(s"[TagPropagation] Governed tag winner: $key")
    }

    var winnerApplied = false
    val resolvedPlans = cleanedPlans.map { p =>
      val filteredTags = p.tagsToPropagate.filter { t =>
        if (!governedTagKeys.contains(t.key)) true
        else if (winnerKey.contains(t.key) && !winnerApplied) { winnerApplied = true; true }
        else false
      }
      p.copy(tagsToPropagate = filteredTags)
    }

    // Paso 3: blocked si Silver tenía governed tags pero Gold no va a tener ninguno
    val goldAlreadyHasGoverned = existingGoldTags.values.flatten.exists(t => governedTagKeys.contains(t.key))
    val goldWillHaveGoverned   = resolvedPlans.exists(_.tagsToPropagate.exists(t => governedTagKeys.contains(t.key)))

    if (silverGovernedTagKeys.nonEmpty && !goldAlreadyHasGoverned && !goldWillHaveGoverned && goldColumns.nonEmpty) {
      val lostTag   = governedTagPriority.find(silverGovernedTagKeys.contains).getOrElse(silverGovernedTagKeys.head)
      val blocked   = Tag(TagPropagationService.BlockedTagKey, lostTag)
      val firstCol  = goldColumns.head
      val firstTags = existingGoldTags.getOrElse(firstCol, Seq.empty)

      if (!firstTags.exists(_.key == TagPropagationService.BlockedTagKey)) {
        val goldRef   = TableRef(goldCatalog, goldSchema, goldTable)
        val tableName = plans.headOption.map(_.targetColumn.fullTableName).getOrElse(goldRef.full)
        logger.warn(s"[TagPropagation] BLOCKED: governed tag '$lostTag' from Silver was lost in $tableName — applying blocked tag to column '$firstCol'")
        var patched = false
        val result = resolvedPlans.map { p =>
          if (p.targetColumn.columnName == firstCol) {
            patched = true
            p.copy(tagsToPropagate = p.tagsToPropagate :+ blocked)
          } else p
        }
        return if (patched) result
        else result :+ TagPropagationPlan(
          targetColumn    = ColumnInfo(goldRef.catalog, goldRef.schema, goldRef.table, firstCol, firstTags),
          tagsToPropagate = Seq(blocked),
          sourceColumns   = Seq.empty
        )
      }
    }

    resolvedPlans
  }

  /**
   * Genera los comandos SQL ALTER TABLE para propagar los tags.
   */
  def generateTagPropagationSQL(plan: Seq[TagPropagationPlan]): Seq[String] = {
    plan.flatMap { p =>
      p.tagsToPropagate.map { tag =>
        s"""ALTER TABLE ${p.targetColumn.fullTableName} ALTER COLUMN ${p.targetColumn.columnName} SET TAGS ('${tag.key}' = '${tag.value}');"""
      }
    }
  }

  /**
   * Genera un DataFrame con el resumen de propagación.
   */
  def generatePropagationReport(plan: Seq[TagPropagationPlan]): DataFrame = {
    val sparkSession = spark
    import sparkSession.implicits._

    plan.flatMap { p =>
      if (p.tagsToPropagate.isEmpty && p.sourceColumns.flatMap(_.tags).isEmpty) {
        Seq.empty
      } else {
        val sourceSummary = p.sourceColumns.map { src =>
          s"${src.fullColumnName} [${src.tags.map(t => s"${t.key}=${t.value}").mkString(", ")}]"
        }.mkString("; ")

        val existingTags = p.targetColumn.tags.map(t => s"${t.key}=${t.value}").mkString(", ")
        val newTags = p.tagsToPropagate.map(t => s"${t.key}=${t.value}").mkString(", ")

        Seq((
          p.targetColumn.fullTableName,
          p.targetColumn.columnName,
          existingTags,
          newTags,
          sourceSummary,
          p.tagsToPropagate.nonEmpty
        ))
      }
    }.toDF(
      "gold_table",
      "gold_column",
      "existing_tags",
      "tags_to_propagate",
      "silver_sources",
      "needs_update"
    )
  }

  /**
   * Ejecuta la propagación de tags (aplica los ALTER TABLE).
   */
  def executeTagPropagation(plan: Seq[TagPropagationPlan], dryRun: Boolean = true): Seq[(String, Boolean, String)] = {
    val sqlCommands = generateTagPropagationSQL(plan)

    logger.info(s"[TagPropagation] Executing ${sqlCommands.size} ALTER TABLE statement(s) (dryRun=$dryRun)")

    val results = sqlCommands.map(sql => runSql(sql, dryRun))

    val (ok, ko) = results.partition(_._2)
    logger.info(s"[TagPropagation] Done — ${ok.size} succeeded, ${ko.size} failed")
    results
  }
}

/**
 * Plan de propagación junto con el resumen del linaje Silver con el que se ha calculado.
 *
 * @param plans              plan de propagación
 * @param lineageSourceCount número total de fuentes Silver encontradas en el linaje de todas las columnas Gold
 * @param columnsWithLineage columnas Gold con al menos una fuente Silver
 */
private[tag_propagation] case class PlanWithLineage(plans: Seq[TagPropagationPlan], lineageSourceCount: Int,
                                                    columnsWithLineage: Set[String])

/** Constantes de la propagación de tags compartidas por el servicio y el cálculo del diff. */
object TagPropagationService {
  /** Governed tags (excluyentes entre sí), por orden de prioridad. */
  val GovernedTagPriority: Seq[String] = Seq("management_company", "logistic_center")
  /** Keys de los governed tags. */
  val GovernedTagKeys: Set[String] = GovernedTagPriority.toSet
  /** Key del tag que bloquea la tabla cuando se pierde un governed tag de Silver. */
  val BlockedTagKey: String = "blocked"
}
