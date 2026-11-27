package com.gestamp.hypatia.adls

import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.spark.SparkOptions
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

/**
 * Contrato de acceso a ADLS Gen2: configuración OAuth de Spark y lectura/escritura
 * de datos del lago. Implementación: ADLS2Manager.
 */
trait IADLS2Manager {

  val tenantGestamp: String
  val clientId: String
  val clientSecret: String

  /**
   * Configura en Spark la autenticación OAuth contra las cuentas de ADLS Gen2.
   *
   * @return la configuración de Hadoop resultante
   */
  def setADLS2OAuthSparkConfiguration: Configuration

  /**
   * Escribe el DataFrame en el path indicado y lo registra como tabla Delta.
   *
   * @param deltaTableName nombre de la tabla destino
   * @param dataFrame      datos a escribir
   * @param path           ruta física destino
   * @param loadType       tipo de carga (p.ej. saveAsTable)
   * @param sparkOptions   opciones de escritura (formato, saveMode, particiones...)
   * @return Right(Unit) si termina bien, o Left con el error
   */
  def sparkWriteData(deltaTableName: String, dataFrame: DataFrame, path: String, loadType: String, sparkOptions: SparkOptions): Either[Throwable, Unit]

  /**
   * Lee datos del path indicado con las opciones dadas.
   *
   * @param path         ruta a leer
   * @param sparkOptions opciones de lectura (formato, cabecera, delimitador...)
   * @return el DataFrame leído
   */
  def sparkReadData(path: String, sparkOptions: SparkOptions): DataFrame

  /**
   * Lee los ficheros de la capa raw del dataset a un DataFrame según su formato.
   *
   * @param source  source del dataset
   * @param dataset dataset a leer
   * @return el DataFrame leído
   */
  def readRawFilesToDF(source: String, dataset: Dataset): DataFrame

}
