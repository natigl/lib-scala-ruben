package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.Subdomain

/**
 * Contrato de consultas sobre la tabla subdomain. Implementación: SubdomainDao.
 */
trait iSubdomainDao {

  /** Devuelve el subdominio validado con ese código. */
  def getSubdomainByCode(code: String): Subdomain

}
