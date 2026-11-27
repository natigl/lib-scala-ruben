package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.LatestProcessedVersions

/**
 * Contrato de escritura sobre latest_processed_versions. Implementación: LatestProcessedVersionsDao.
 */
trait iLatestProcessedVersions {
  /** Inserta la última versión procesada del flujo o la actualiza si ya existe (upsert por clave única). */
  def insertIntoOnConflict(insertModel: LatestProcessedVersions): Unit
}
