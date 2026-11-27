package com.gestamp.hypatia.postgresql.model

case class Distillation(id: Int,
                        datum: String,
                        key_columns: String,
                        ingest_type: String,
                        partition_by: String)
