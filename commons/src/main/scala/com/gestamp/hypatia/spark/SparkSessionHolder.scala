package com.gestamp.hypatia.spark

import org.apache.spark.sql.SparkSession

/**
 * Contenedor thread-safe de la SparkSession compartida por la librería,
 * independiente de la ActiveSession thread-local de Spark.
 */
object SparkSessionHolder {
  @volatile private var current: Option[SparkSession] = None

  /** Registra la SparkSession a compartir. */
  def set(session: SparkSession): Unit = {
    current = Some(session)
  }

  /** Devuelve la SparkSession registrada, si la hay. */
  def get: Option[SparkSession] = current

  /** Elimina la SparkSession registrada. */
  def clear(): Unit = {
    current = None
  }
}
