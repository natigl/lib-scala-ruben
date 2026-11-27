package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.TagExclusion

/**
 * Contrato de consultas sobre tag_exclusion. Implementación: TagExclusionDao.
 */
trait iTagExclusionDao {

  /** Devuelve las exclusiones de tags activas. */
  def getActiveExclusions: Seq[TagExclusion]
}