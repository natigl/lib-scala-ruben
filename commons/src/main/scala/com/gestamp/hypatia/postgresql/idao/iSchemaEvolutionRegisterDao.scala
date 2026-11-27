package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.SchemaEvolutionRegister

/**
 * Contrato de consultas sobre la tabla schema_evolution_register.
 * Implementación: SchemaEvolutionRegisterDao.
 */
trait iSchemaEvolutionRegisterDao {

  /** Devuelve la acción de evolución de esquema con ese id. */
  def getActionByActionId(action_id: Int): SchemaEvolutionRegister

  /** Devuelve las acciones de evolución de esquema del dataset con ese id. */
  def getActionsByDatasetId(dataset_id: Int): Seq[SchemaEvolutionRegister]

  /** Devuelve las acciones de evolución de esquema del dataset con ese nombre. */
  def getActionsByDatasetName(dataset_name: String): Seq[SchemaEvolutionRegister]

}
