package com.gestamp.hypatia.postgresql.auth

import java.sql.ResultSet

/**
 * Adapta un ResultSet de JDBC a un Iterator de Scala aplicando la función de
 * mapeo fila a fila, y permite materializarlo en una lista.
 *
 * @param resultSet       cursor JDBC a recorrer
 * @param resultExtractor función de mapeo fila → modelo
 * @tparam T modelo al que se mapea cada fila
 */
class MapperResultIterator[T](resultSet: ResultSet, resultExtractor: ResultSet => T) {

  private val castToIterator = new Iterator[T] {
    /** Avanza el cursor: hay siguiente elemento si el ResultSet tiene otra fila. */
    override def hasNext = resultSet.next()
    /** Mapea la fila actual del cursor al modelo. */
    override def next() = resultExtractor(resultSet)
  }

  /**
   * Materializa el ResultSet completo en una lista aplicando el mapeo.
   *
   * @return todas las filas mapeadas al modelo
   */
  def getList: Seq[T] = castToIterator.toList
}
