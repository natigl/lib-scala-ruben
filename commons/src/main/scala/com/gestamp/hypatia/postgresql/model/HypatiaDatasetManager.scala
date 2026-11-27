package com.gestamp.hypatia.postgresql.model

case class HypatiaDatasetManager(hypt_process: String,
                                 library: String,
                                 main_class: String,
                                 min_workers: Int,
                                 max_workers: Int,
                                 custom_params: Option[Map[String, Any]],
                                 usecase_id: String)
