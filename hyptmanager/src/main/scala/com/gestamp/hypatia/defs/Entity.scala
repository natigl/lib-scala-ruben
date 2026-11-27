package com.gestamp.hypatia.defs

import com.gestamp.hypatia.spark.SparkOptions
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

/**
 * An abstract class that defines the structure of an entity.
 */
abstract class Entity {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val targetDeltaSpace: DeltaSpace

  val sourceDeltaSpaces: List[DeltaSpace]

  /**
   * Calcula el DataFrame de la entidad gold a partir de los DataFrames de sus
   * espacios Delta origen. Lo implementa cada entidad concreta.
   *
   * @param dataframes DataFrames origen indexados por su DeltaSpace
   * @return el DataFrame de la entidad calculada
   */
  def calculateGoldEntity(dataframes: Map[DeltaSpace, DataFrame]): DataFrame

  /**
   * Opciones de escritura de la entidad en la capa gold. Por defecto sobreescribe la
   * tabla completa (SaveMode.Overwrite). Para añadir filas en lugar de sobreescribir,
   * se puede sobreescribir con HypatiaSparkOptions.appendTable() o appendPartitionTable(...):
   * {{{
   *   override lazy val sparkOptions: SparkOptions = HypatiaSparkOptions.appendPartitionTable(List("date"))
   * }}}
   * En modo append, relanzar el proceso vuelve a insertar las mismas filas: evitar
   * duplicados es responsabilidad de calculateGoldEntity.
   */
  lazy val sparkOptions: SparkOptions = HypatiaSparkOptions.fullTable
}
