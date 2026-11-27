package com.gestamp.hypatia.postgresql.idao

import com.gestamp.hypatia.postgresql.model.Schema

/**
 * Contrato de consultas sobre la tabla schema. Implementación: SchemaDao.
 */
trait iSchemaDao {

  /** Devuelve el esquema con ese id. */
  def getSchemaById(id: Int): Schema

}
