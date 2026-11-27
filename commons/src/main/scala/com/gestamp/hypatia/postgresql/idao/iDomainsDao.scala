package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.Domain

/**
 * Contrato de consultas sobre la tabla domain. Implementación: DomainDao.
 */
trait iDomainsDao {

  /** Devuelve el dominio validado con ese código. */
  def getDomainByCode(code: String): Domain

}
