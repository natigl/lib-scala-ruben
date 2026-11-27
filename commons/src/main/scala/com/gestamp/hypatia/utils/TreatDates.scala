package com.gestamp.hypatia.utils

import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.{Failure, Success, Try}

object TreatDates {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val formatter = new SimpleDateFormat(CommonsConstants.DATE_FORMAT)

  /**
   *
   * @param dataDate
   * @return
   */
  def castDateData(dataDate: String): String = {
    logger.info("Start method to validate format dataDate")

    Try(formatter.parse(dataDate)) match {
      case Success(value) => value.toString
      case Failure(exception) => {
        if(!dataDate.isEmpty) {
          logger.error("Error validando y parseando dataDate")
          throw exception
        } else
          dataDate
      }
    }
  }

  /**
   *
   * @param dateStr
   * @return
   */
  def isValidDateFormat(dateStr: String): Boolean = {
    formatter.setLenient(false)

    Try(formatter.parse(dateStr)) match {
      case Success(value) => true
      case Failure(exception) => {
        logger.error("Error validando y parseando dataDate")
        throw exception
      }
    }
  }

  /**
   * TODO: Evolutivo para castear el dataDate params
   * @return
   */
  def getTodayDateFormatted(): (String, String, String) = {
    val today = formatter.format(Calendar.getInstance().getTime())

    val day = today.split("/")(0)
    val month = today.split("/")(1)
    val year = today.split("/")(2)

    (day, month, year)
  }

}
