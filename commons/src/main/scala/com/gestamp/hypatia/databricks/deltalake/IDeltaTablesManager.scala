package com.gestamp.hypatia.databricks.deltalake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Contrato de operaciones sobre tablas Delta (DDL, mantenimiento y lectura).
 * Implementación por defecto: [[DeltaTablesManager]].
 */
trait IDeltaTablesManager {

  /**
   * Crea la base de datos (schema de Unity Catalog) si no existe.
   *
   * @param database nombre de la base de datos a crear
   * @return Right con el nombre de la base de datos, o Left con el error
   */
  def createDatabase(database: String): Either[Throwable, String]

  /**
   * Crea una tabla Delta en la localización dada si no existe, opcionalmente con esquema inicial.
   *
   * @param table    nombre de la tabla
   * @param location ruta física donde se almacena la tabla
   * @param squema   esquema inicial de la tabla; si se omite se crea sin columnas
   * @return Right con el nombre de la tabla, o Left con el error
   */
  def createDeltaTable(table: String, location: String, squema: StructType = StructType(Nil)): Either[Throwable, String]

  /**
   * Optimiza (compacta) la tabla Delta; si se indica un campo, aplica ZORDER BY sobre él.
   *
   * @param table nombre/ruta de la tabla a optimizar
   * @param field campo para el Z-ordering; vacío para compactación normal
   * @return Right con la tabla optimizada, o Left con el error
   */
  def optimizeDeltaTable(table: String, field: String): Either[Throwable, String]

  /**
   * Ejecuta VACUUM sobre la tabla Delta para eliminar snapshots antiguos.
   *
   * @param table ruta de la tabla
   * @return Right(true) si termina bien, o Left con el error
   */
  def vacuumDeltaTable(table: String): Either[Throwable, Boolean]

  /**
   * Restaura la tabla Delta a una versión anterior (RESTORE VERSION AS OF).
   *
   * @param path    ruta de la tabla
   * @param version versión a la que restaurar
   * @return Right(true) si termina bien, o Left con el error
   */
  def revertToWriteDeltaTable(path: String, version: Long): Either[Throwable, Boolean]

  /**
   * Muestra por consola el detalle (DESCRIBE DETAIL) de la tabla Delta.
   *
   * @param path ruta de la tabla
   */
  def detailDeltaTable(path: String)

  /**
   * Crea o reemplaza una vista a partir de una vista temporal de Spark.
   *
   * @param viewName      nombre completo de la vista (catalogo.schema.vista)
   * @param tempSparkView nombre de la vista temporal de Spark origen
   * @return Right con el nombre de la vista, o Left con el error
   */
  def createView(viewName: String, tempSparkView: String): Either[Throwable, String]

  /**
   * Cambia el owner del objeto Delta al grupo de administradores correspondiente al entorno.
   *
   * @param securableType tipo de objeto (SCHEMA, TABLE, ...)
   * @param securableName nombre del objeto
   * @return Right con el nombre del objeto, o Left con el error
   */
  def changePermissionsTeamAdmins(securableType: String, securableName: String): Either[Throwable, String]

  /**
   * Lee el contenido completo de una tabla Delta como DataFrame.
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @return Right con el DataFrame, o Left con el error
   */
  def readDeltaTable(catalog: String, schema: String, deltaObj: String): Either[Throwable, DataFrame]

  /**
   * Borra filas de una tabla Delta; si la condición está vacía borra todas las filas.
   *
   * @param catalog   catálogo que contiene la tabla
   * @param schema    esquema que contiene la tabla
   * @param deltaObj  nombre de la tabla
   * @param condition condición SQL de borrado; vacía para borrar todo
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def deleteDeltaTable(catalog: String, schema: String, deltaObj: String, condition: String = ""): Either[Throwable, Unit]

  /**
   * Establece propiedades (TBLPROPERTIES) en una tabla Delta.
   *
   * @param catalog    catálogo que contiene la tabla
   * @param schema     esquema que contiene la tabla
   * @param deltaObj   nombre de la tabla
   * @param properties lista de propiedades clave=valor a establecer
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def setPropertiesDeltaTable(catalog: String, schema: String, deltaObj: String, properties: List[String]): Either[Throwable, Unit]

  /**
   * Elimina una columna de una tabla Delta.
   *
   * @param catalog    catálogo que contiene la tabla
   * @param schema     esquema que contiene la tabla
   * @param deltaObj   nombre de la tabla
   * @param columnName columna a eliminar
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def dropColumnDeltaTable(catalog: String, schema: String, deltaObj: String, columnName:String): Either[Throwable, Unit]

  /**
   * Reorganiza la tabla Delta (REORG TABLE ... APPLY), por defecto con PURGE.
   *
   * @param catalog     catálogo que contiene la tabla
   * @param schema      esquema que contiene la tabla
   * @param deltaObj    nombre de la tabla
   * @param where       condición WHERE opcional
   * @param applyClause cláusula APPLY a ejecutar (por defecto PURGE)
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def reorgDeltaTable(catalog: String, schema: String, deltaObj: String, where: String = "", applyClause: String = "PURGE"): Either[Throwable, Unit]

  /**
   * Añade una columna a una tabla Delta.
   *
   * @param catalog    catálogo que contiene la tabla
   * @param schema     esquema que contiene la tabla
   * @param deltaObj   nombre de la tabla
   * @param columnName columna a añadir
   * @param columnType tipo de la nueva columna (por defecto STRING)
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def addColumnDeltaTable(catalog: String, schema: String, deltaObj: String, columnName: String, columnType: String = "STRING"): Either[Throwable, Unit]

  /**
   * Renombra una columna de una tabla Delta.
   *
   * @param catalog       catálogo que contiene la tabla
   * @param schema        esquema que contiene la tabla
   * @param deltaObj      nombre de la tabla
   * @param columnName    columna a renombrar
   * @param newColumnName nuevo nombre de la columna
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def renameColumnDeltaTable(catalog: String, schema: String, deltaObj: String, columnName: String, newColumnName: String): Either[Throwable, Unit]

  /**
   * Cambia el tipo de una columna reescribiendo la tabla Delta con el cast aplicado.
   *
   * @param catalog    catálogo que contiene la tabla
   * @param schema     esquema que contiene la tabla
   * @param deltaObj   nombre de la tabla
   * @param columnName columna a retipar
   * @param columnType tipo destino de la columna
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def typeColumnDeltaTable(catalog: String, schema: String, deltaObj: String, columnName: String, columnType: String): Either[Throwable, Unit]

/*  def ActiveChangeDataFeed(Deltatable: String): Either[Throwable, String]

  def readLatestVersionDeltaTable(catalog: String, schema: String, deltaObj: String, path: String, extractType: String): Either[Throwable, DataFrame]

  def latestVersionDeltaTable(deltaTablePath: String): Long

  def upsertDeltaTable(dfTarget: DeltaTable, dfSource: DataFrame, keyColumns: Seq[String]): Either[Throwable, Unit]

  def upsertDeltaTableQuality(deltaTarget: DeltaTable, dfSource: DataFrame, keyColumns: Seq[String]): Either[Throwable, Unit]
*/

}
