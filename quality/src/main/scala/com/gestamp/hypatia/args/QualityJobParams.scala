package com.gestamp.hypatia.args

import com.gestamp.hypatia.postgresql.model.Dataset

/**
 * Clase que contiene los parámetros de entrada necesarios para la ejecución del modulo de calidad
 * @param source
 * @param datasets datasets ya resueltos contra la BD por (nombre, source). Es el campo que consume el job.
 * @param datasetNames campo interno del parseo: los nombres crudos que llegan en el -d. Un mismo nombre de
 *                     dataset puede existir en varios sources, así que no se puede resolver contra la BD hasta
 *                     conocer también el -o (source). Como scopt procesa los argumentos en el orden de la línea
 *                     de comandos (el -o puede llegar después del -d), la acción del -d solo guarda aquí los
 *                     nombres y QualityParserArgs.parserArgs los resuelve tras el parse. En el objeto final que
 *                     recibe el job este campo queda vacío.
 */
case class QualityJobParams(source: String = "",
                            datasets: Seq[Dataset] = List.empty,
                            datasetNames: Seq[String] = List.empty)

case class HistoricalQualityJobParams(source: String = "",
                            datasets: Seq[Dataset] = List.empty,
                            delta_version: Option[Int] = None
                            )

