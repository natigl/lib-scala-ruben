package com.gestamp.hypatia.utils

import com.gestamp.hypatia.postgresql.model.Domain
import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

class JsonToolsTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val testingParamsJSON = "{\"param1\": \"value1\", \"param2\": true, \"param3\": 900}"
  val testingParamsJSON_2 = "{\"param1\": \"value1\", \"param2\": \"true\", \"param3\": \"900\"}"
  val testingParamsJSON_3 = "[{\"code\": \"HDC\", \"description\": \"Headcount\", \"validated\": true, \"promotion\": false}, " +
    "{\"code\": \"EMP\", \"description\": \"Employes\", \"validated\": true, \"promotion\": false}]"

  test("Get Parameters from JSON String: Map[String, Any]") {
    logger.info(s"Test looooooggggggggerrrr")
    val paramsParsing = JsonTools.parseJsonParameters(testingParamsJSON)

    assert(paramsParsing.get("param1").equals("value1"))
    assert(paramsParsing.get("param2") == true)
    assert(paramsParsing.get("param3") == 900)
  }


  test("Get Parameters from JSON String: Map[String, String]") {
    val paramsParsing = JsonTools.jsonStrToMap(testingParamsJSON_2)

    assert(paramsParsing.get("param1").get.equals("value1"))
    assert(paramsParsing.get("param2").get.equals("true"))
    assert(paramsParsing.get("param3").get.equals("900"))
  }


  test("Get List of case class from JSON String: List[A]") {
    val paramsParsing = JsonTools.jsonStrToList[Domain](testingParamsJSON_3)

    assert(paramsParsing.size == 2)
    assert(paramsParsing.head.code.equals("HDC"))
    assert(paramsParsing(1).description.equals("Employes"))
  }

}
