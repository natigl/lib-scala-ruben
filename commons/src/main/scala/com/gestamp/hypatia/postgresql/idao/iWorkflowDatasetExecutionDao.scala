package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.WorkflowDatasetExecution

/**
 * Contrato de escritura sobre workflow_dataset_execution. Implementación: WorkflowDatasetExecutionDao.
 */
trait iWorkflowDatasetExecutionDao {
  /** Inserta el registro de procesamiento de un dataset dentro de una task. */
  def insertInto(insertModel: WorkflowDatasetExecution): Unit
}
