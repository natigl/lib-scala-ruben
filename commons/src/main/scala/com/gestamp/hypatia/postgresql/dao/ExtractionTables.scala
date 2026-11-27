package com.gestamp.hypatia.postgresql.dao

import com.gestamp.hypatia.postgresql.auth.PostgreSqlManager
import org.slf4j.{Logger, LoggerFactory}

import java.sql.ResultSet
import scala.collection.concurrent.TrieMap

/**
 * Tablas de extracción que relacionan un source con un dataset a través del par (source, dataset_id).
 * Se utilizan para resolver un dataset único a partir de (nombre, source), dado que el nombre de
 * dataset puede repetirse entre sources distintos.
 *
 * Las tablas no están hardcodeadas: se descubren consultando information_schema, buscando en el esquema
 * las tablas cuyo nombre termina en "_extraction" y que tienen las columnas source y dataset_id (esta
 * segunda condición evita traer tablas que solo coinciden en el nombre). Así, al crear una tabla de
 * extracción para un nuevo origen no hay que tocar este objeto. El resultado se cachea por esquema
 * durante la vida del proceso.
 */
object ExtractionTables {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val tablesBySchema = TrieMap.empty[String, Seq[String]]

  private class InformationSchemaDao(schema: String) extends PostgreSqlManager[String](schema) {

    /**
     * Mapea cada fila del information_schema al nombre de la tabla.
     *
     * @param resultSet cursor posicionado en la fila a mapear
     * @return el nombre de la tabla
     */
    override protected def mapperRowdata(resultSet: ResultSet): String = resultSet.getString("table_name")

    /**
     * Consulta en information_schema las tablas *_extraction del esquema que
     * tienen las columnas source y dataset_id.
     *
     * @return los nombres de las tablas de extracción encontradas
     */
    def extractionTables(): Seq[String] = runSelect(
      s"""SELECT table_name FROM information_schema.columns
         | WHERE table_schema = '$schema'
         | and table_name LIKE '%\\_extraction'
         | and column_name in ('source', 'dataset_id')
         | GROUP BY table_name
         | HAVING count(distinct column_name) = 2""".stripMargin)
  }

  /**
   * Devuelve las tablas de extracción del esquema, descubriéndolas la primera vez
   * y sirviéndolas de caché después.
   *
   * @param schema esquema donde buscar las tablas
   * @return los nombres de las tablas de extracción
   * @throws IllegalStateException si no se encuentra ninguna tabla de extracción en el esquema
   */
  def tables(schema: String): Seq[String] = tablesBySchema.getOrElseUpdate(schema, {
    val discovered = new InformationSchemaDao(schema).extractionTables()
    if (discovered.isEmpty)
      throw new IllegalStateException(
        s"No extraction tables found in schema '$schema' (tables named *_extraction with source and dataset_id columns)")
    logger.info(s"Discovered extraction tables in schema '$schema': ${discovered.mkString(", ")}")
    discovered
  })

  /**
   * SQL con los dataset_id asociados a un source, uniendo todas las tablas de extracción.
   * Pensado para usarse como subquery: WHERE dataset_id in (...)
   */
  def datasetIdsBySourceSql(schema: String, source: String): String =
    tables(schema).map(table => s"SELECT dataset_id FROM $schema.$table WHERE source = '$source'").mkString(" UNION ALL ")

}
