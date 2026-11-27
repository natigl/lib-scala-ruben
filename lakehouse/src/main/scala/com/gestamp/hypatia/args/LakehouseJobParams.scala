package com.gestamp.hypatia.args


/**
 * Clase que contiene los parámetros de entrada necesarios para la ejecución del modulo de Lakehouse.
 * @param tables: Tablas sobre las que se van a realizar las "actions".
 * @param target_layer: Capa donde se encuentran las tablas especificadas.
 * @param source: Origen de los datasets involucrados en las acciones realizadas sobre las tablas especificadas.
 * @param action: Acción a realizar.
 * @param schema_evolution_id: ID de la acción del cambio de esquema registrado en PostgreSQL (tabla schema_evolution_register).
 */
case class LakehouseJobParams(tables: Seq[String] = List.empty,
                              target_layer: String = "",
                              action: String = "",
                              source: String = "",
                              schema_evolution_id: Option[Int] = None,
                              delta_version: Option[Int] = None
                              )