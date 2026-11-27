package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.Distillation

/**
 * Contrato de consultas sobre la tabla distillation. Implementación: DistillationDao.
 */
trait iDistillationDao {

  /** Devuelve la distillation con ese id. */
  def getDistillationById(id: Int): Distillation

  /** Devuelve las distillations con esos ids. */
  def getDistillationBySequenceIds(ids: Seq[Int]): Seq[Distillation]

  /** Devuelve la distillation con ese nombre (datum). */
  def getDistillationByName(name: String): Distillation

  /** Devuelve las distillations de un dominio y subdominio. */
  def getDistillationByDomainSubdomain(domainCode: String, subdomainCode: String): Seq[Distillation]

  /** Devuelve la distillation de un dominio/subdominio y nombre de tabla, o null si no existe. */
  def getDistillationByDomainSubdomainAndTable(domainCode: String, subdomainCode: String, table: String): Distillation
}
