package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.BlobExtraction

/**
 * Contrato de consultas sobre la tabla blob_extraction. Implementación: BlobExtractionDao.
 */
trait iBlobExtractionDao {

  /** Devuelve la primera extracción activa cuyo dataset tenga ese nombre. */
  def getBlobExtractionByName(name: String): BlobExtraction

  /** Devuelve las extracciones activas cuyos datasets tengan esos nombres. */
  def getBlobExtractionBySequenceNames(names: Seq[String]): Seq[BlobExtraction]

  /** Devuelve la extracción activa con ese id. */
  def getBlobExtractionById(id: Int): BlobExtraction

  /** Devuelve las extracciones activas asociadas a esos dataset_id. */
  def getBlobExtractionBySequenceIds(ids: Seq[Int]): Seq[BlobExtraction]

}
