package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.ColumnTag

/**
 * Contrato de consultas sobre column_tag. Implementación: ColumnTagDao.
 */
trait iColumnTagDao {

  /** Devuelve todos los tags de columna definidos. */
  def getAllColumnTags: Seq[ColumnTag]
}
