package com.gestamp.hypatia.defs

import com.gestamp.hypatia.spark.SparkOptions
import com.gestamp.hypatia.utils.CommonsConstants
import org.apache.spark.sql.SaveMode

object HypatiaSparkOptions {

  val fullTable: SparkOptions = SparkOptions(
    fileFormat = CommonsConstants.DELTA_FORMAT, 
    saveMode = SaveMode.Overwrite,
    otherOptions = Map("overwriteSchema" -> "true"))

  /**
   * Opciones de escritura Delta con sobreescritura total y particionado por las
   * columnas indicadas.
   *
   * @param columns columnas de partición
   * @return las SparkOptions correspondientes
   */
  def partitionTable(columns: List[String] ):SparkOptions = SparkOptions(
    fileFormat = CommonsConstants.DELTA_FORMAT,
    saveMode = SaveMode.Overwrite,
    partitionBy = columns,
    otherOptions = Map("overwriteSchema" -> "true"))

  /**
   * Opciones de escritura Delta en modo append: añade las filas a la tabla gold
   * existente (o la crea si no existe) sin borrarla previamente.
   *
   * @param mergeSchema si es true, permite añadir columnas nuevas al esquema de la tabla
   * @return las SparkOptions correspondientes
   */
  def appendTable(mergeSchema: Boolean = false): SparkOptions = SparkOptions(
    fileFormat = CommonsConstants.DELTA_FORMAT,
    saveMode = SaveMode.Append,
    otherOptions = mergeSchemaOption(mergeSchema))

  /**
   * Opciones de escritura Delta en modo append y particionado por las columnas
   * indicadas.
   *
   * @param columns     columnas de partición
   * @param mergeSchema si es true, permite añadir columnas nuevas al esquema de la tabla
   * @return las SparkOptions correspondientes
   */
  def appendPartitionTable(columns: List[String], mergeSchema: Boolean = false): SparkOptions = SparkOptions(
    fileFormat = CommonsConstants.DELTA_FORMAT,
    saveMode = SaveMode.Append,
    partitionBy = columns,
    otherOptions = mergeSchemaOption(mergeSchema))

  private def mergeSchemaOption(mergeSchema: Boolean): Map[String, String] =
    if (mergeSchema) Map("mergeSchema" -> "true") else Map.empty
}
