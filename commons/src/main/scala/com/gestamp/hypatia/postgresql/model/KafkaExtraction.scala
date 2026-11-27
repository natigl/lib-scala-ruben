package com.gestamp.hypatia.postgresql.model

case class KafkaExtraction(topic: String,
                   schema: String,
                   startingOffset: String,
                   isToLand: Boolean,
                   maxOffsetsPerTrigger: Int)
