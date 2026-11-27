package com.gestamp.hypatia.defs

/**
 * Represents a table signature within a specific data space.
 * This case class is used to encapsulate the identifying features of a table, including its catalogue, schema, and name.
 * It serves as a unified reference point for accessing tables across different data processing tasks.
 *
 * @param catalogue The catalogue under which the table is categorized. Often used to group tables within a broader data domain or source.
 * @param schema The schema name that organizes the table within the catalogue. Schemas help in logically grouping tables of similar or related data.
 * @param name The unique name of the table within its schema. This is the identifier used to directly access or reference the table in data operations.
 */
case class DeltaSpace(catalogue: String = "", schema: String = "", name: String)
