package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.Dataset

/**
 * Contrato de consultas sobre la tabla dataset. Implementación: DatasetDao.
 */
trait iDatasetDao {

  /** Devuelve el primer dataset activo con ese nombre (no desambigua entre sources). */
  def getDatasetByName(name: String): Dataset

  /** Resuelve el dataset único activo por (nombre, source) vía las tablas de extracción. */
  def getDatasetByName(name: String, source: String): Dataset

  /** Devuelve los datasets activos con esos nombres (no desambigua entre sources). */
  def getDatasetsBySequenceNames(names: Seq[String]): Seq[Dataset]

  /** Devuelve los datasets activos con esos nombres que pertenezcan al source dado. */
  def getDatasetsBySequenceNames(names: Seq[String], source: String): Seq[Dataset]

  /** Devuelve el dataset activo con ese id. */
  def getDatasetById(id: Int): Dataset

  /** Devuelve los datasets activos con esos ids. */
  def getDatasetsBySequenceIds(ids: Seq[Int]): Seq[Dataset]

}
