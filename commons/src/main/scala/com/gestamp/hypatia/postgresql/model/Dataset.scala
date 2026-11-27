package com.gestamp.hypatia.postgresql.model

case class Dataset(id: Int,
                   dataset: String,
                   extract_type: String,
                   delta_column: String,
                   format: String,
                   blocksizeinmb: Int,
                   mappings: String,
                   header: Boolean,
                   trigger_id: Int,
                   active: Boolean,
                   group: String,
                   delimiter: String,
                   key_columns: String)
