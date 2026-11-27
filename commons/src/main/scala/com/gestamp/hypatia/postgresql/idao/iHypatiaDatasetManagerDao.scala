package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.HypatiaDatasetManager

/**
 * Contrato de consultas sobre la tabla hypatia_dataset_manager.
 * Implementación: HypatiaDatasetManagerDao.
 */
trait iHypatiaDatasetManagerDao {

  /** Devuelve la configuración del proceso de Hypatia indicado. */
  def getHyptManagerByProcess(hypt_process: String): HypatiaDatasetManager

  /*def getHyptManagerbyUseCase(usecase_id: Integer): Seq[Hyptmanager]

  def getHyptManagerByGroupId(group_id: Integer): Seq[Hyptmanager]

  def getHyptManagerByLibrary(library: String): Seq[Hyptmanager]*/

}
