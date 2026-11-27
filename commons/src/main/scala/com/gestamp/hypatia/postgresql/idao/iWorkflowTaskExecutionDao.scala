package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.WorkflowTaskExecution

/**
 * Contrato de escritura sobre workflow_task_execution. Implementación: WorkflowTaskExecutionDao.
 */
trait iWorkflowTaskExecutionDao {
  /** Inserta la ejecución de la task o la actualiza si ya existe (upsert por task_run_id). */
  def insertIntoOnConflict(insertModel: WorkflowTaskExecution): Unit
}
