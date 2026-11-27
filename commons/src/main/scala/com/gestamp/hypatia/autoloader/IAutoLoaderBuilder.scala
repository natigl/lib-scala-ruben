package com.gestamp.hypatia.autoloader

import com.gestamp.hypatia.adls.ADLS2Manager
import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.spark.SparkOptions
import org.apache.spark.sql.DataFrame

/**
 * Contrato de las operaciones de streaming del Auto Loader (lectura de landing
 * y escritura/merge en raw_sources). Implementación: [[AutoLoader]].
 */
trait IAutoLoaderBuilder  {

  val configsDataLake = ADLS2Manager.setADLS2OAuthSparkConfiguration()

  /**
   * Abre el stream de lectura de Auto Loader (cloudFiles) con la configuración dada.
   *
   * @param autoloaderConfig opciones de cloudFiles (formato, path, esquema...)
   * @return Right con el DataFrame de streaming, o Left con el error
   */
  def sparkReadStream(autoloaderConfig: Map[String, String]): Either[Throwable, DataFrame]

  /**
   * Escribe el stream del dataset en su tabla de raw_sources (modo append/overwrite
   * según las opciones), aplicando la deduplicación propia de cada (source, extract_type).
   *
   * @param dataset      dataset a ingestar
   * @param dataFrame    DataFrame de streaming leído de landing
   * @param path         ruta física destino de la tabla
   * @param sparkOptions opciones de escritura (formato, saveMode, particiones...)
   * @param source       source del dataset
   * @return Right con (filas leídas, filas escritas), o Left con el error
   */
  def sparkWriteStream(dataset: Dataset, dataFrame: DataFrame, path: String, sparkOptions: SparkOptions, source: String): Either[Throwable, (Long, Long)]

  /**
   * Escribe el stream del dataset haciendo merge (upsert CDC) sobre la tabla
   * Delta existente en raw_sources.
   *
   * @param dataset   dataset a ingestar
   * @param dataFrame DataFrame de streaming leído de landing
   * @param path      ruta de la tabla Delta destino
   * @param source    source del dataset
   * @return Right con (filas leídas, filas escritas), o Left con el error
   */
  def sparkWriteStreamMerge(dataset: Dataset, dataFrame: DataFrame, path: String, source: String): Either[Throwable, (Long,Long)]

}
