package com.gestamp.hypatia.integration.operations

import org.apache.spark.sql.DataFrame

/**
 * Contrato de las operaciones de combinación de DataFrames usadas por la
 * integración (union, unionByName y joins). Implementación: [[TaxonomyOperations]].
 */
trait ITaxonomyOperations {

  /** Une los DataFrames por nombre de columna, permitiendo columnas ausentes. */
  def unionByNameOperation(dataFrames: Seq[DataFrame]): DataFrame

  /** Une los DataFrames por posición de columna (union clásica). */
  def unionOperation(dataFrames: Seq[DataFrame]): DataFrame

  /** Hace join de los DataFrames con el tipo de join y la expresión de igualdad indicados. */
  def joinOperation(operation: String, expr: String, dataFrames: Seq[DataFrame]): DataFrame

}
