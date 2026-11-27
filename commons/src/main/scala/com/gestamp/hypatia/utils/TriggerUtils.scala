package com.gestamp.hypatia.utils

import org.apache.spark.sql.streaming.Trigger
import com.gestamp.hypatia.utils.CommonsConstants

/**
 * Utilidad para resolver el Trigger de Spark Structured Streaming según el tipo de extracción.
 */
object TriggerUtils {

  /**
   * Devuelve el trigger a usar: AvailableNow para extracciones streaming,
   * Once para el resto.
   *
   * @param triggerType tipo de extracción
   * @return el Trigger correspondiente
   */
    def getTrigger(triggerType: String): Trigger = {
    triggerType match {
    case CommonsConstants.STREAM_EXTRACTION => Trigger.AvailableNow()
    case _               => Trigger.Once()
  }
}
}