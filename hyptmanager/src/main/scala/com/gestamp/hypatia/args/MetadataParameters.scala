package com.gestamp.hypatia.args

case class MetadataParameters(useCase: String = "",
                              dataDate: String = "",
                              group: String = "",
                              bronzePaths: Map[String, String],
                              silverPaths: Map[String, String],
                              goldPath: String,
                              spread:  Boolean = false,
                              silverDeltaTable: Map[String, String],
                              goldDeltaTable: Map[String, String])
