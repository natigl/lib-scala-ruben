package com.gestamp.hypatia.postgresql.model

case class Quality(id: Int,
                   name: String,
                   dataset_id: Int,
                   rule: String,
                   value: String,
                   fields: String,
                   description: String,
                   active: Boolean,
                   replacement: String)
