package com.gestamp.hypatia.postgresql.model

case class BlobExtraction(id: Int,
                          source: String,
                          dataset_id: Int,
                          minidle: Int,
                          maxworkers:Int,
                          poolname: String,
                          path: String,
                          active: Boolean)
