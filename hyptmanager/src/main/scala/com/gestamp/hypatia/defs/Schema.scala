package com.gestamp.hypatia.defs

import org.apache.spark.sql.types.StructType

/**
 * An abstract class that defines the schema of a delta table.
 */
abstract class Schema {

  val deltaSpace: DeltaSpace
  val schema: StructType
  /** Devuelve los nombres de las columnas del esquema como lista. */
  def toList: List[String] = this.schema.names.toList
}
