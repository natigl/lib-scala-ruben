package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.DataRelationship

/**
 * Contrato de consultas sobre la tabla data_relationship (relaciones dataset →
 * distillation). Implementación: DataRelationshipDao.
 */
trait iDataRelationshipDao {

  /** Devuelve las relaciones de la distillation con ese id. */
  def getDataRelationshipByDistillationId(distillation_id: Int): Seq[DataRelationship]

  /** Devuelve las relaciones de las distillations con esos ids. */
  def getDataRelationshipBySeqDistillationId(distillation_ids: Seq[Int]): Seq[DataRelationship]

  /** Devuelve las relaciones de los datasets con esos ids. */
  def getDataRelationshipBySeqDatasetId(dataset_ids: Seq[Int]): Seq[DataRelationship]

  /** Devuelve las relaciones de los datasets con esos nombres (no desambigua entre sources). */
  def getDataRelationshipByDatasetName(dataset_names: Seq[String]): Seq[DataRelationship]

  /** Devuelve las relaciones de los datasets con esos nombres que pertenezcan al source dado. */
  def getDataRelationshipByDatasetName(dataset_names: Seq[String], source: String): Seq[DataRelationship]

  /** Devuelve las relaciones de las distillations con esos nombres. */
  def getDataRelationshipByDistillationName(distillation_names: Seq[String]): Seq[DataRelationship]
}
