package com.gestamp.hypatia.databricks

import com.databricks.backend.daemon.dbutils.FileInfo
import com.databricks.dbutils_v1.DBUtilsV1

import scala.util.Try


/**
 * Contrato de operaciones sobre el entorno Databricks vía dbutils: secretos,
 * montaje/desmontaje de contenedores del lago (staging, bronze, silver, gold)
 * y utilidades de filesystem (ls, rm, copy).
 */
trait IDatabricksManager {

  val dbutils: DBUtilsV1

  /**
   * Recupera un secreto del secret scope de Databricks.
   *
   * @param keyName nombre de la clave del secreto
   * @return el valor del secreto
   */
  def dbUtilsGetSecret(keyName: String): String

  /**
   * Monta el contenedor de staging en el path indicado si no está ya montado.
   *
   * @param configs configuración de autenticación del montaje
   * @param path    punto de montaje destino
   * @return Right con el path montado, o Left con el error
   */
  def dbUtilsFsMountStaging(configs: Map[String, String], path: String): Either[Throwable, String]

  /**
   * Monta el contenedor de bronze en el path indicado si no está ya montado.
   *
   * @param configs configuración de autenticación del montaje
   * @param path    punto de montaje destino
   * @return Right con el path montado, o Left con el error
   */
  def dbUtilsFsMountBronze(configs: Map[String, String], path: String): Either[Throwable, String]

  /**
   * Ejecuta el montaje físico del contenedor indicado sobre la capa bronze.
   *
   * @param configs   configuración de autenticación del montaje
   * @param path      punto de montaje destino
   * @param container contenedor de ADLS a montar
   * @return Try con true si el montaje termina bien
   */
  def mountPathBronze(configs: Map[String, String], path: String, container: String): Try[Boolean]

  /**
   * Monta el contenedor de silver en el path indicado si no está ya montado.
   *
   * @param configs configuración de autenticación del montaje
   * @param path    punto de montaje destino
   * @return el path montado
   */
  def dbUtilsFsMountSilver(configs: Map[String, String], path: String): String

  /**
   * Ejecuta el montaje físico del contenedor de silver.
   *
   * @param configs configuración de autenticación del montaje
   * @param path    punto de montaje destino
   * @return Try con true si el montaje termina bien
   */
  def mountPathSilver(configs: Map[String, String], path: String): Try[Boolean]

  /**
   * Monta el contenedor de gold en el path indicado si no está ya montado.
   *
   * @param configs configuración de autenticación del montaje
   * @param path    punto de montaje destino
   * @return el path montado
   */
  def dbUtilsFsMountGold(configs: Map[String, String], path: String): String

  /**
   * Ejecuta el montaje físico del contenedor de gold.
   *
   * @param configs configuración de autenticación del montaje
   * @param path    punto de montaje destino
   * @return Try con true si el montaje termina bien
   */
  def mountPathGold(configs: Map[String, String], path: String): Try[Boolean]

  /**
   * Copia ficheros entre dos paths de DBFS/ADLS.
   *
   * @param fromPath path origen
   * @param toPath   path destino
   * @return el path destino
   */
  def dbUtilsCopyFiles(fromPath: String, toPath: String): String

  /**
   * Ejecuta la copia física recursiva entre dos paths.
   *
   * @param fromPath path origen
   * @param toPath   path destino
   * @return Try con true si la copia termina bien
   */
  def fsCopy(fromPath: String, toPath: String): Try[Boolean]

  /**
   * Desmonta el punto de montaje indicado.
   *
   * @param path punto de montaje a desmontar
   * @return Right(true) si termina bien, o Left con el error
   */
  def dbUtilsFsUnmountPath(path: String): Either[Throwable, Boolean]

  /**
   * Lista el contenido de un path (equivalente a dbutils.fs.ls).
   *
   * @param path path a listar
   * @return la lista de ficheros/directorios del path
   */
  def dbUtilsFsLs(path: String): Seq[FileInfo]

  /**
   * Borra recursivamente el path indicado (equivalente a dbutils.fs.rm).
   *
   * @param path path a borrar
   */
  def dbUtilsFsRm(path: String): Unit

  /**
   * Deduce el formato de fichero (csv, parquet, json, ...) a partir del contenido del path.
   *
   * @param path path donde están los ficheros
   * @return la extensión/formato detectado
   */
  def getFileFormat(path: String): String

}
