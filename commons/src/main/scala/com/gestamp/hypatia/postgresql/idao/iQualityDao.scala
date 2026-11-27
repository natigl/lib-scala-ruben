package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.Quality

/**
 * Contrato de consultas sobre la tabla quality (reglas de calidad por dataset).
 * Implementación: QualityDao.
 */
trait iQualityDao {

  /** Devuelve las reglas de calidad activas del dataset con ese id. */
  def getRulesById(dataset_id: Int): Seq[Quality]

  /** Devuelve la primera regla de calidad activa del dataset con ese id. */
  def getRuleById(dataset_id: Int): Quality

  /** Devuelve las reglas de calidad de los datasets con esos ids. */
  def getRulesBySequenceIds(dataset_id: Seq[Int]): Seq[Quality]

}
