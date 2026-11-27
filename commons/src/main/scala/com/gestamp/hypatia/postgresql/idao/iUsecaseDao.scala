package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.Usecase

/**
 * Contrato de consultas sobre la tabla usecase. Implementación: UsecaseDao.
 */
trait iUsecaseDao {

  /** Devuelve el caso de uso con ese id. */
  def getUsecaseByUsecaseId(id: Int): Usecase

  /** Devuelve los casos de uso de un área. */
  def getUsecaseByArea(area: String): Seq[Usecase]

  /** Devuelve los casos de uso de un programa. */
  def getUsecaseByProgram(program: String): Seq[Usecase]

  /** Devuelve el caso de uso asociado a un proceso de Hypatia. */
  def getUsecaseByHyptProcess(hypt_process: String): Usecase

}
