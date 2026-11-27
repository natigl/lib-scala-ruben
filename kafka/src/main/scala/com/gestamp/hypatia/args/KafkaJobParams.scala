package com.gestamp.hypatia.args

import com.gestamp.hypatia.postgresql.model.Dataset

/**
 * Clase que contiene los parámetros de entrada necesarios para la ejecución del modulo de calidad
 * @param source
 * @param dataset dataset ya resuelto contra la BD por (nombre, source). Es el campo que consume el job.
 * @param datasetName campo interno del parseo: el nombre crudo que llega en el -d. Un mismo nombre de
 *                    dataset puede existir en varios sources, así que no se puede resolver contra la BD hasta
 *                    conocer también el -o (source). Como scopt procesa los argumentos en el orden de la línea
 *                    de comandos (el -o puede llegar después del -d), la acción del -d solo guarda aquí el
 *                    nombre y KafkaParserArgs.parserArgs lo resuelve tras el parse. En el objeto final que
 *                    recibe el job este campo queda vacío.
 */
case class KafkaJobParams(source: String = "",
                            dataset: Dataset = null,
                            datasetName: String = "")

