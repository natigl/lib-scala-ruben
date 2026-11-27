package com.gestamp.hypatia.postgresql.model

case class SchemaEvolutionRegister(action_id: Int,
                                   action_name: String,
                                   dataset_id: Int,
                                   dataset_name: String,
                                   column: String,
                                   new_name: String,
                                   column_type: String,
                                   landing_regeneration: Boolean,
                                   registration_date: String,
                                   execution_date: String,
                                  )