package com.gestamp.hypatia.utils

import com.gestamp.hypatia.exceptions.JsonToolsException
import org.apache.spark.sql.types.StructType
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object JsonTools {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  case class SchemaFormat(fieldName: String, fieldType: String, isNullable: Boolean)

  /**
   * Method that parsers a JSON String to a Case Class
   * @param jsonStr
   * @param en
   * @tparam A
   * @return
   */
  def jsonToCaseClass[A](jsonStr: String)(implicit en: scala.reflect.Manifest[A]): A = {
    logger.info(s"Start parse method from json string to Case Class nested: $jsonStr")

    implicit val formats = DefaultFormats
    Try(parse(jsonStr).extract[A]) match {
      case Success(value) => value
      case Failure(exception) =>
        throw new JsonToolsException(s"Error casting json to list, json: $jsonStr", exception)
    }
  }

  /**
   *
   * @param jsonStr
   * @param en
   * @tparam A
   * @return
   */
  def jsonStrToList[A](jsonStr: String)(implicit en: scala.reflect.Manifest[A]): List[A] = {
    logger.info(s"Start parseArgs method from json string to List[Case Class]: " +
      s"\n dataset json = $jsonStr")

    implicit val formats = DefaultFormats
    Try(parse(jsonStr).extract[List[A]]) match {
      case Success(value) => value
      case Failure(exception) => {
        throw new JsonToolsException(s"Error casting json to list, json: $jsonStr", exception)
      }
    }
  }

  /**
   *
   * @param jsonStr
   * @return
   */
  def jsonStrToMap(jsonStr: String): Map[String, String] = {
    logger.info(s"Start parseArgs method from json string to Map[String, String]: " +
      s"\n dataset json = $jsonStr")

    implicit val formats = DefaultFormats
    Try(parse(jsonStr).extract[Map[String, String]]) match {
      case Success(value) => value
      case Failure(exception) => {
        throw new JsonToolsException(s"Error casting json to map, json: $jsonStr", exception)
      }
    }
  }

  /**
   * Parsea un JSON de parámetros custom a un mapa opcional clave → valor.
   *
   * @param jsonParameters JSON con los parámetros (puede ser null/vacío → None)
   * @return el mapa de parámetros, si lo hay
   * @throws JsonToolsException si el JSON no se puede parsear
   */
  def parseJsonParameters(jsonParameters: String): Option[Map[String, Any]] = {
    logger.info(s"Start parse Json Parameters json = $jsonParameters")

    //if(!jsonParameters.equals("")) {
    implicit val formats = DefaultFormats
    Try(parse(jsonParameters).extract[Option[Map[String, Any]]]) match {
      case Success(value) => value
      case Failure(exception) => throw new JsonToolsException(s"Error casting json to map, json: $jsonParameters", exception)
    }
    //}
  }

  /**
   * Construye el StructType de Spark a partir de un JSON de esquema con el
   * formato [{fieldName, fieldType, isNullable}, ...].
   *
   * @param dataset JSON con la definición del esquema
   * @return el StructType equivalente
   */
  def buildStructType(dataset: String): StructType = {
    logger.info(s"Start cast schema to StructType: $dataset")

    val listSchema: List[SchemaFormat] = jsonStrToList[SchemaFormat](dataset)

    logger.info("Start cast List to StructType")
    val castion = listSchema.foldLeft(new StructType()) {
      (structType, field) => structType.add(field.fieldName, field.fieldType, field.isNullable)
    }

    castion
  }

}
