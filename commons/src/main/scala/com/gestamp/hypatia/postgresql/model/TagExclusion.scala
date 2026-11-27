package com.gestamp.hypatia.postgresql.model

case class TagExclusion(id: Long,
                        domain_code: String,
                        subdomain_code: String,
                        table_name: String,
                        object_type: String)