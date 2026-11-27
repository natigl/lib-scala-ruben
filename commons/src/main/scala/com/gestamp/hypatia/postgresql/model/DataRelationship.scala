package com.gestamp.hypatia.postgresql.model

case class DataRelationship(dataset_id: Int,
                            dataset_name: String,
                            distillation_id: Int,
                            distillation_name: String,
                            operation: String,
                            fields: String,
                            select: String)
