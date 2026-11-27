package com.gestamp.hypatia.utils.tag_propagation

// Domain Classes
case class Tag(key: String, value: String)

case class ColumnInfo(
                       catalog: String,
                       schema: String,
                       tableName: String,
                       columnName: String,
                       tags: Seq[Tag] = Seq.empty
                     ) {
  /** Nombre completo de la tabla: catalogo.schema.tabla. */
  def fullTableName: String = s"$catalog.$schema.$tableName"
  /** Nombre completo de la columna: catalogo.schema.tabla.columna. */
  def fullColumnName: String = s"$fullTableName.$columnName"
}

case class ColumnLineageWithTags(
                                  goldColumn: ColumnInfo,
                                  silverSources: Seq[ColumnInfo]
                                )

case class TagPropagationPlan(
                               targetColumn: ColumnInfo,
                               tagsToPropagate: Seq[Tag],
                               sourceColumns: Seq[ColumnInfo]
                             )
