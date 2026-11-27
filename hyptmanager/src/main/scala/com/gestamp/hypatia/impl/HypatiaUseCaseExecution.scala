package com.gestamp.hypatia.impl

import com.gestamp.hypatia.HypatiaDataset
import com.gestamp.hypatia.defs.{DeltaSpace, Entity, UseCaseProcess}
import com.gestamp.hypatia.utils.CommonsConstants
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

/**
 * Represents a Hypatia process.
 *
 * @constructor create a new Hypatia with args and associated process.
 * @param args the command line arguments passed to the application.
 * @param process UseCaseProcess that defines the entities to generate and the execution mode(parallel or sequential).
 */
class HypatiaUseCaseExecution(args: Array[String], process: UseCaseProcess) extends HypatiaDataset(args: Array[String]) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Genera todas las entidades gold del proceso, en paralelo o en secuencia
   * según la configuración del UseCaseProcess.
   */
  override def propagateToGold(): Unit = {
    if (process.parallelProcessing) { process.entities.par.foreach { entity => createGoldTable(entity) } }
    else { process.entities.foreach { entity => createGoldTable(entity)}}
  }

  /**
   * Crea la tabla gold de una entidad: lee los DataFrames de sus espacios Delta
   * origen, calcula la entidad y la escribe en la capa gold.
   *
   * @param entity entidad a generar
   */
  private def createGoldTable(entity: Entity): Unit = {
    logger.info(s"${entity.targetDeltaSpace} -> Start process...")
    val dataframes: Map[DeltaSpace, DataFrame] = entity.sourceDeltaSpaces.map { hypatiaTable => hypatiaTable -> readDataFromDeltaTable(hypatiaTable.name, (hypatiaTable.catalogue + CommonsConstants.ENV_SOURCE, hypatiaTable.schema)) }.toMap
    writeDataToDeltaTable(entity.targetDeltaSpace.name, entity.calculateGoldEntity(dataframes), goldPath, entity.sparkOptions)
  }
  
}
