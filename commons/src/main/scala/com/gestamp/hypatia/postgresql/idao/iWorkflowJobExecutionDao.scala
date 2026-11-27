package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.WorkflowJobExecution

/**
 * Contrato de escritura sobre workflow_job_execution. Implementación: WorkflowJobExecutionDao.
 */
trait iWorkflowJobExecutionDao {
  /** Inserta la ejecución del job o la actualiza si ya existe (upsert por job_run_id). */
  def insertIntoOnConflict(insertModel: WorkflowJobExecution): Unit
}
