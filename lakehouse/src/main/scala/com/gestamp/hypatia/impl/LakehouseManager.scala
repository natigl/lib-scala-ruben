package com.gestamp.hypatia.impl

import com.gestamp.hypatia.adls.ADLS2Manager.setADLS2OAuthSparkConfiguration
import com.gestamp.hypatia.args.LakehouseJobParams
import com.gestamp.hypatia.utils.{CommonsConstants, ConstantsLakehouse}
import org.slf4j.{Logger, LoggerFactory}
import com.gestamp.hypatia.logic.LakehouseDefault
import com.gestamp.hypatia.utils._
import com.gestamp.hypatia.postgresql.dao.SchemaEvolutionRegisterDao
import com.gestamp.hypatia.postgresql.model.SchemaEvolutionRegister
import com.gestamp.hypatia.exceptions.LakehouseManagerException

import scala.util.{Failure, Success, Try}


object LakehouseManager {
  private val schemaEvolutionDao: SchemaEvolutionRegisterDao = new SchemaEvolutionRegisterDao
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val lakehouseDefault = new LakehouseDefault {}

  /**
   * Orquesta las acciones de lakehouse: aplica en paralelo la acción pedida
   * (recarga histórica, recarga por versión, evolución de esquema, actualización
   * de esquema por CSV...) sobre cada tabla en la capa destino.
   *
   * @param jobParams parámetros del job (tablas, capa destino, source, acción e ids opcionales)
   * @return mensaje de fin del proceso
   */
  def handleData(jobParams: LakehouseJobParams): String = {
    logger.info(s"Start lakehouse handleData method.\n" +
      s"Tables: ${jobParams.tables.mkString(", ")}\n" +
      s"Target layer: ${jobParams.target_layer}\n" +
      s"Source: ${jobParams.source}\n" +
      s"Action: ${jobParams.action}\n" +
      s"Schema evolution id: ${jobParams.schema_evolution_id.getOrElse("Not specified")}\n" +
      s"Delta version for reloading: ${jobParams.delta_version.getOrElse("Not specified")}"
    )

    setADLS2OAuthSparkConfiguration()

    jobParams.tables.par.foreach { table =>
      logger.info(s"Applying Lakehouse action ${jobParams.action} on $table, in ${jobParams.target_layer} layer")
        executeLakehouseAction(table, s"${jobParams.target_layer}${CommonsConstants.ENV_SOURCE}", jobParams.action, jobParams.source, jobParams.schema_evolution_id, jobParams.delta_version)
      logger.info(s"Action done (${jobParams.action}) for table $table")
      }
    "End Lakehouse Actions"
  }


  /**
   * Executes a lakehouse action on a table
   *
   * @param table Table on which the action is performed
   * @param layer Layer to which the table belongs
   * @param action Action performed
   * @param source Origin of the datasets involved
   * @param schemaEvolutionId Schema evolution id
   * @param deltaVersion Delta version for reloading
   *
   * @return Nothing
   * @throws Exception if the action applied fails
   */
  private def executeLakehouseAction(table: String, layer: String, action: String, source:String, schemaEvolutionId: Option[Int] = None, deltaVersion: Option[Int] = None): Unit = {
    logger.info(s"Running $action lakehouse action on the $table table, in $layer layer")
    action match {
      case ConstantsLakehouse.TOTAL_HISTORICAL_TABLE_RELOAD =>
        if(layer == CommonsConstants.RAW_SOURCES){
          val schemaEvolutionRows = schemaEvolutionDao.getActionsByDatasetName(table)
          val coalescedColumnPairs= getCoalescePairs(schemaEvolutionRows)
          logger.info(s"Coalesce columns pairs found: $coalescedColumnPairs")
          lakehouseDefault.total_historical_reload(table, layer, source, coalescedColumnPairs)
        } else
          lakehouseDefault.total_historical_reload(table, layer, source)
      case ConstantsLakehouse.UPDATE_SCHEMA_BY_CSV => lakehouseDefault.update_schema_integration(table, layer, source)
      case ConstantsLakehouse.DELETE_COLUMN | ConstantsLakehouse.ADD_COLUMN | ConstantsLakehouse.RENAME_COLUMN | ConstantsLakehouse.COLUMN_TYPING =>
        val evolutionId = schemaEvolutionId match {
          case Some(id) =>
            logger.info(s"Schema evolution id: $id")
            id
          case None => throw new LakehouseManagerException("schema_evolution_id argument needs to provide a value")
        }
        handleSchemaEvolutionAction(table, layer, action, source, evolutionId)
      case ConstantsLakehouse.DELTA_VERSION_TABLE_RELOAD =>
        val reloadVersion = deltaVersion match {
          case Some(id) =>
            logger.info(s"Delta version for reloading: $id")
            id
          case None => throw new LakehouseManagerException("delta_version argument needs to provide a value")
        }
        lakehouseDefault.delta_version_reload(table, layer, source, reloadVersion)
      case _ => logger.info(s"$action action doesn't exist in HypatIA platform, so nothing was done")
    }
  }

  /**
   * Ejecuta una acción de evolución de esquema (delete/add/rename/cast de columna):
   * recupera el registro por schema_evolution_id, lo valida contra la tabla y la
   * acción pedidas y aplica el cambio.
   *
   * @param table             tabla sobre la que actuar
   * @param layer             capa a la que pertenece la tabla
   * @param action            acción de evolución a aplicar
   * @param source            source de la tabla
   * @param schemaEvolutionId id del registro de evolución de esquema
   */
  private def handleSchemaEvolutionAction(table: String, layer: String, action: String, source:String, schemaEvolutionId: Int): Unit = {
    val schemaEvolutionRegister = Try(schemaEvolutionDao.getActionByActionId(schemaEvolutionId)) match {
      case Success(row) => row
      case Failure(e) =>
        throw new LakehouseManagerException("The corresponding row could not be retrieved from PostgreSQL through the schema_evolution_id provided.", e)
    }
    validateSchemaEvolution(schemaEvolutionRegister, table, action)
    action match {
      case ConstantsLakehouse.DELETE_COLUMN =>
        lakehouseDefault.delete_column(table, layer, source, schemaEvolutionRegister.column)
      case ConstantsLakehouse.ADD_COLUMN =>
        lakehouseDefault.add_column(table, layer, source, schemaEvolutionRegister.column, schemaEvolutionRegister.column_type)
      case ConstantsLakehouse.RENAME_COLUMN =>
        lakehouseDefault.rename_column(table, layer, source, schemaEvolutionRegister.column, schemaEvolutionRegister.new_name)
      case ConstantsLakehouse.COLUMN_TYPING =>
        lakehouseDefault.cast_column(table, layer, source, schemaEvolutionRegister.column, schemaEvolutionRegister.column_type)
    }
  }

  /**
   * Valida que el registro de evolución de esquema recuperado corresponde a la
   * tabla y acción que se van a procesar; si no, lanza excepción.
   *
   * @param schemaEvolutionRegister registro recuperado de la BD
   * @param datasetName             tabla a procesar
   * @param actionName              acción a procesar
   */
  private def validateSchemaEvolution(schemaEvolutionRegister: SchemaEvolutionRegister, datasetName: String, actionName: String): Unit = {
    if (schemaEvolutionRegister.dataset_name == datasetName && schemaEvolutionRegister.action_name == actionName) {
      logger.info("The schema evolution registration has been validated.")
    } else {
      throw new LakehouseManagerException(s"The row obtained from PostgreSQL through the schema_evolution_id does not correspond" +
        s" to the table and/or action to be processed.\n" +
        s"Schema evolution id: ${schemaEvolutionRegister.action_id}\n" +
        s"Registered table on PostgreSQL: ${schemaEvolutionRegister.dataset_name} --> Table to be processed: $datasetName.\n" +
        s"Registered action on PostgreSQL: ${schemaEvolutionRegister.action_name} --> Action to be processed: $actionName\n")
    }
  }

  /**
   * Obtiene los pares (columna vieja, columna nueva) de los renombrados de columna
   * pendientes de regenerar en landing, recorriendo el histórico de evolución de
   * esquema de más reciente a más antiguo y parando en el primer registro ya regenerado.
   *
   * @param schemaEvolutionRows registros de evolución de esquema del dataset
   * @return los pares de columnas a coalescer en la recarga histórica
   */
  private def getCoalescePairs(schemaEvolutionRows: Seq[SchemaEvolutionRegister]): List[CoalescePair] = {
    val sortedRows = schemaEvolutionRows.sortBy(-_.action_id)
    sortedRows
      .foldLeft(List.empty[CoalescePair]) { (result, row) =>
        if (row.landing_regeneration) {
          return result
        } else if (row.action_name == "rename_column" && !row.landing_regeneration) {
          CoalescePair(row.column, row.new_name) :: result
        } else {
          result
        }
      }
  }

  /**
   * Marca el fin del job de lakehouse (solo log).
   *
   * @return mensaje de fin del job
   */
  def finishLakehouseActions(): String = {
    logger.info("End Lakehouse Databricks Job")
    "End Lakehouse Databricks Job"
  }

}

