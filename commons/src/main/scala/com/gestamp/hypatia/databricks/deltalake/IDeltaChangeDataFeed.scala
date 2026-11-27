package com.gestamp.hypatia.databricks.deltalake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame

/**
 * Contrato de operaciones sobre el Change Data Feed (CDF) de tablas Delta:
 * activación, consulta de versiones y lectura/merge de cambios.
 * Implementación por defecto: [[DeltaChangeDataFeed]].
 */
trait IDeltaChangeDataFeed {

  /**
   * Activa el Change Data Feed en la tabla Delta indicada.
   *
   * @param Deltatable nombre de la tabla
   * @return Right con el nombre de la tabla, o Left con el error
   */
  def ActiveChangeDataFeed(Deltatable: String): Either[Throwable, String]

  /**
   * Devuelve la última versión de la tabla Delta a partir de su ruta en ADLS.
   *
   * @param deltaTablePath ruta de la tabla
   * @return el número de la última versión
   */
  def getLatestDeltaVersion(deltaTablePath: String): Long

  /**
   * Devuelve la última versión "significativa" de la tabla: la última operación
   * que modifica datos (WRITE, MERGE, UPDATE, DELETE, ...), ignorando operaciones
   * de mantenimiento como OPTIMIZE o VACUUM.
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @return el número de la última versión con cambios de datos
   */
  def getLatestSignificantDeltaVersion(catalog: String, schema: String, deltaObj: String): Long

  /**
   * Lee del CDF los cambios de la última versión de la tabla incluyendo operaciones
   * CDC (insert, update y delete; se excluye update_preimage).
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @param path     ruta de la tabla (para resolver la última versión)
   * @return Right con el DataFrame de cambios, o Left con el error
   */
  def readDeltaFeedFromLastVersionCDC(catalog: String, schema: String, deltaObj: String, path: String): Either[Throwable, DataFrame]

  /**
   * Lee del CDF los cambios de la última versión de la tabla quedándose solo
   * con las operaciones de insert (escenario full/append).
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @param path     ruta de la tabla (para resolver la última versión)
   * @return Right con el DataFrame de cambios, o Left con el error
   */
  def readDeltaFeedFromLastVersion(catalog: String, schema: String, deltaObj: String, path: String): Either[Throwable, DataFrame]

  /**
   * Lee del CDF los cambios de una versión concreta de la tabla, solo inserts.
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @param version  versión a leer
   * @return Right con el DataFrame de cambios, o Left con el error
   */
  def readDeltaFeedFromVersion(catalog: String, schema: String, deltaObj: String, version: Long): Either[Throwable, DataFrame]

  /**
   * Lee del CDF los cambios de una versión concreta de la tabla incluyendo
   * operaciones CDC (se excluye update_preimage).
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @param version  versión a leer
   * @return Right con el DataFrame de cambios, o Left con el error
   */
  def readDeltaFeedFromVersionCDC(catalog: String, schema: String, deltaObj: String, version: Long): Either[Throwable, DataFrame]

  /**
   * Hace un merge (upsert + delete) de los cambios CDC del DataFrame origen
   * sobre la tabla Delta destino usando las columnas clave como condición de join.
   *
   * @param deltaTarget tabla Delta destino
   * @param dfSource    DataFrame origen con los cambios
   * @param keyColumns  columnas clave para la condición de join del merge
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def upsertDeltaTableChangeDataCapture(deltaTarget: DeltaTable, dfSource: DataFrame, keyColumns: Seq[String]): Either[Throwable, Unit]

}
