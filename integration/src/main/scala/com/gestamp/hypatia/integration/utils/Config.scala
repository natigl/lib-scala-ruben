package com.gestamp.hypatia.integration.utils

case class Config(taxonomy: Taxonomy = null,
                  schema: String = "")

case class Taxonomy(operation: String = "",
                    leftTable: Stage = null,
                    rightTable: Stage = null,
                    expr: String = "",
                    select: String = "")

case class Stage(operation: String = "",
                 dataset: DatasetTag = null,
                 leftTable: Stage = null,
                 rightTable: Stage = null,
                 expr: String = "",
                 select: String = "")

case class DatasetTag(id: Int = 0,
                   name: String = "",
                   select: String = "")


