package com.gestamp.hypatia.utils.tag_propagation

/**
 * Identificador de una tabla de Unity Catalog (catalogo.esquema.tabla) ya normalizado.
 *
 * Unity Catalog guarda los nombres de catálogo, esquema y tabla en MINÚSCULAS: una tabla registrada como
 * `HeadcountActual` se almacena como `headcountactual`. Las consultas a las system tables
 * (`system.information_schema.*`, `system.access.column_lineage`) y a la API REST de linaje tienen que usar el
 * nombre tal y como lo guarda UC, o no encuentran nada (y entonces no se propaga ningún tag).
 *
 * Este tipo centraliza esa normalización en un ÚNICO punto: la única forma de construir un `TableRef` es a través
 * de [[TableRef.apply]] / [[TableRef.fromFull]], que pasan los identificadores a minúsculas. Así, cualquier método
 * que reciba un `TableRef` tiene la garantía (impuesta por el compilador, no por disciplina) de que el nombre ya
 * está normalizado: es imposible pasarle un `String` sin normalizar.
 *
 * Los nombres de COLUMNA no se normalizan aquí a propósito: UC preserva su caja original (p. ej.
 * `managementCompany_code`) y las sentencias `ALTER COLUMN` deben usarla tal cual. Las comparaciones por nombre de
 * columna se hacen sin distinguir mayúsculas usando [[TableRef.normalize]], que es la misma función de
 * normalización reutilizada por el resto del flujo (claves de caché, lista de exclusión, filtro de capa…).
 */
final class TableRef private (val catalog: String, val schema: String, val table: String) {

  /** Nombre completo `catalogo.esquema.tabla`, ya normalizado. */
  def full: String = s"$catalog.$schema.$table"

  override def toString: String = full

  override def equals(other: Any): Boolean = other match {
    case that: TableRef => catalog == that.catalog && schema == that.schema && table == that.table
    case _ => false
  }

  override def hashCode: Int = (catalog, schema, table).##
}

object TableRef {

  /** Crea un `TableRef` normalizando catálogo, esquema y tabla a como los almacena Unity Catalog (minúsculas). */
  def apply(catalog: String, schema: String, table: String): TableRef =
    new TableRef(normalize(catalog), normalize(schema), normalize(table))

  /**
   * Crea un `TableRef` a partir del nombre completo `catalogo.esquema.tabla`.
   *
   * @throws IllegalArgumentException si el nombre no tiene exactamente tres partes
   */
  def fromFull(fullName: String): TableRef = {
    val parts = if (fullName == null) Array.empty[String] else fullName.split('.')
    require(parts.length == 3, s"Expected a 'catalog.schema.table' name, got: $fullName")
    apply(parts(0), parts(1), parts(2))
  }

  /**
   * Normaliza un identificador de Unity Catalog (catálogo, esquema, tabla o columna) a como UC lo compara: en
   * minúsculas. Punto único de normalización reutilizado tanto por [[apply]] como por el resto del flujo
   * (comparación de columnas, lista de exclusión, filtro de capa…). `null` se devuelve tal cual.
   */
  def normalize(id: String): String = if (id == null) null else id.toLowerCase
}
