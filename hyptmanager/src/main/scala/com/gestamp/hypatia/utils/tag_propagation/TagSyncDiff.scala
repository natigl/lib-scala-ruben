package com.gestamp.hypatia.utils.tag_propagation

/** Tag a poner o quitar en una columna de la tabla Gold. */
case class ColumnTagChange(columnName: String, tag: Tag)

/**
 * Diferencia entre los column tags deseados y los actuales de una tabla Gold.
 *
 * @param toSet   tags deseados que faltan o cuyo valor ha cambiado (se aplican PRIMERO)
 * @param toUnset tags actuales que no están en el estado deseado (se quitan DESPUÉS, solo si todos los SET van bien)
 * @param keptByMissingLineage tags actuales que no están en el deseado pero NO se quitan porque su columna no tiene
 *                             ninguna fuente Silver en el linaje (guarda por columna; solo se rellena si se pasa
 *                             `columnsWithLineage` a [[TagSyncDiff.compute]] y `unsetAllowed` es true)
 */
case class TagSyncDiff(toSet: Seq[ColumnTagChange], toUnset: Seq[ColumnTagChange],
                       keptByMissingLineage: Seq[ColumnTagChange] = Seq.empty)

/**
 * Cálculo puro (sin Spark) del diff de column tags entre el estado deseado y el actual de una tabla Gold.
 */
object TagSyncDiff {

  /**
   * Calcula los SET y UNSET necesarios para llevar los column tags actuales al estado deseado.
   *
   * - Solo se consideran las columnas que existen en la tabla (`tableColumns`); los tags de columnas que ya no
   *   existen (p. ej. tras overwriteSchema) se ignoran. Los nombres de columna se comparan sin distinguir
   *   mayúsculas, como Unity Catalog, y en las sentencias se usa el nombre tal cual viene en `tableColumns`.
   * - Si el plan trae varios valores para la misma (columna, key), gana el último, igual que al ejecutar los
   *   SET en orden.
   * - Las keys de tag se comparan tal cual.
   *
   * @param desired      plan deseado (tagsToPropagate de cada columna); vacío = la tabla no debe tener tags
   * @param current      column tags actuales de la tabla Gold, por columna
   * @param tableColumns columnas actuales de la tabla Gold
   * @param unsetAllowed false para no quitar ningún tag (toUnset vacío), ver [[unsetAllowed]]
   * @param columnsWithLineage columnas Gold con al menos una fuente Silver en el linaje (sin distinguir
   *                           mayúsculas). Si se pasa, no se quita ningún tag de una columna que no esté en el
   *                           conjunto: su linaje vacío (latencia o más de daysBack días) no permite saber si el tag
   *                           sigue aplicando, y quitarlo podría dejar la tabla sin row filter; esos tags van a
   *                           `keptByMissingLineage`. None = sin guarda por columna (p. ej. tabla excluida).
   * @param governedKeys       keys de los governed tags. Un governed tag de una columna sin linaje SÍ se quita si el
   *                           deseado contiene algún governed tag (o un tag de `tableLevelKeys`) en alguna columna:
   *                           como los SET van antes, la tabla nunca se queda sin él, y así queda uno solo por tabla
   *                           con la prioridad del plan. Si el deseado no contiene ninguno, se mantiene la guarda.
   * @param tableLevelKeys     keys que son decisiones de tabla (p. ej. `blocked`, calculado con el linaje de todas las
   *                           columnas): quedan fuera de la guarda por columna, solo les aplica `unsetAllowed`.
   * @return el diff, con los SET en el orden del plan y los UNSET ordenados por columna
   */
  def compute(desired: Seq[TagPropagationPlan], current: Map[String, Seq[Tag]],
              tableColumns: Seq[String], unsetAllowed: Boolean = true,
              columnsWithLineage: Option[Set[String]] = None,
              governedKeys: Set[String] = Set.empty,
              tableLevelKeys: Set[String] = Set.empty): TagSyncDiff = {

    val columnsByKey: Map[String, String] = tableColumns.map(c => c.toLowerCase -> c).toMap

    val desiredTags: Seq[((String, String), String)] = desired
      .flatMap(p => p.tagsToPropagate.map(t => (p.targetColumn.columnName.toLowerCase, t.key) -> t.value))
      .filter { case ((col, _), _) => columnsByKey.contains(col) }

    val desiredValues: Map[(String, String), String] = desiredTags.toMap
    val desiredOrder: Seq[(String, String)] = desiredTags.map(_._1).distinct

    val currentTags: Seq[((String, String), String)] = current.toSeq
      .sortBy(_._1.toLowerCase)
      .flatMap { case (col, tags) => tags.map(t => (col.toLowerCase, t.key) -> t.value) }
      .filter { case ((col, _), _) => columnsByKey.contains(col) }

    val currentValues: Map[(String, String), String] = currentTags.toMap

    val toSet = desiredOrder
      .filterNot(k => currentValues.get(k).contains(desiredValues(k)))
      .map { case k @ (col, key) => ColumnTagChange(columnsByKey(col), Tag(key, desiredValues(k))) }

    val unsetCandidates: Seq[(String, String)] = currentTags.map(_._1).distinct.filterNot(desiredValues.contains)

    val lineageColumns: Option[Set[String]] = columnsWithLineage.map(_.map(_.toLowerCase))
    val desiredHasGoverned: Boolean = desiredValues.keys.exists { case (_, key) =>
      governedKeys.contains(key) || tableLevelKeys.contains(key)
    }

    // Keys a las que no aplica la guarda por columna
    def outsideColumnGuard(key: String): Boolean =
      tableLevelKeys.contains(key) || (governedKeys.contains(key) && desiredHasGoverned)

    val (unsettable, kept) = unsetCandidates.partition { case (col, key) =>
      lineageColumns.forall(_.contains(col)) || outsideColumnGuard(key)
    }

    def toChanges(keys: Seq[(String, String)]): Seq[ColumnTagChange] =
      keys.map { case k @ (col, key) => ColumnTagChange(columnsByKey(col), Tag(key, currentValues(k))) }

    val toUnset = if (unsetAllowed) toChanges(unsettable) else Seq.empty
    val keptByMissingLineage = if (unsetAllowed) toChanges(kept) else Seq.empty

    TagSyncDiff(toSet, toUnset, keptByMissingLineage)
  }

  /**
   * Decide si se pueden quitar tags. Una tabla excluida siempre (su estado deseado es "sin tags"). Una tabla no
   * excluida solo si el linaje ha encontrado al menos una fuente Silver en alguna columna Gold: con el linaje
   * totalmente vacío (latencia de system.access.column_lineage o más de daysBack días sin ejecutar) el plan
   * deseado no es fiable y se conservan los tags actuales. Si hay fuentes, aunque no tengan tags, sí se limpia.
   *
   * @param excluded           true si la tabla está en la lista de exclusión
   * @param lineageSourceCount número total de fuentes Silver encontradas en el linaje de todas las columnas Gold
   * @return true si se pueden ejecutar los UNSET
   */
  def unsetAllowed(excluded: Boolean, lineageSourceCount: Int): Boolean = excluded || lineageSourceCount > 0

  /** Sentencia SET TAGS de un cambio, con el mismo formato que la propagación existente. */
  def setTagSQL(fullTableName: String, change: ColumnTagChange): String =
    s"""ALTER TABLE $fullTableName ALTER COLUMN ${change.columnName} SET TAGS ('${change.tag.key}' = '${change.tag.value}');"""

  /** Sentencia UNSET TAGS de un cambio. */
  def unsetTagSQL(fullTableName: String, change: ColumnTagChange): String =
    s"""ALTER TABLE $fullTableName ALTER COLUMN ${change.columnName} UNSET TAGS ('${change.tag.key}');"""
}
