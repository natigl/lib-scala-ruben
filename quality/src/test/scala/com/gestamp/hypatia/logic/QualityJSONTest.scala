package com.gestamp.hypatia.logic

import com.gestamp.hypatia.SparkSessionMock
import com.gestamp.hypatia.spark.SparkConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.slf4j.{Logger, LoggerFactory}

class QualityJSONTest extends FunSuite with Matchers with BeforeAndAfter with SparkSessionMock {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val quality = new QualityJSON()

  mockSparkContext()
  val spark = SparkConfiguration.spark

  import spark.implicits._


  test("jsonToComplexType: should return Country json field as StructType and names as ArrayType ") {


    val inputDF = Seq(
      (1, "[{\"Name\":\"Alice\"},{\"Name\":\"Jhon\"}]", "{  \"Country\": \"Spain\" }", "30", null),
      (2, "[{\"Name\":\"Alice\"}]", "{  \"Country\": \"Spain\" }", null, "qewewqewqeqweqw"),
      (3, "[{\"Name\":\"Jhon\"}]", null, "56", "vxcxcvvxxv")
    ).toDF("id", "names", "country", "age", "email")


    val result: Either[Throwable, DataFrame] = quality.jsonToComplexType(inputDF)

    val dfResult = result.right.get
    result.isRight shouldBe true

    dfResult.printSchema()
    assert(dfResult.count() == 3)
    assert(dfResult.schema.fields.count( f => f.name== "country" && f.dataType.isInstanceOf[StructType] ) == 1)
    assert(dfResult.schema.fields.count( f => f.name== "names" && f.dataType.isInstanceOf[ArrayType] ) == 1)

  }

  test("jsonToComplexType: malformed Names arraytype is skipped ") {

    val inputDF = Seq(
      (1, "{\"Name\":\"Alice\"},{\"Name\":\"Jhon\"}]", "{  \"Country\": \"Spain\" }", "30", null),
      (2, "{\"Name\":\"Alice\"}]", "{  \"Country\": \"Spain\" }", null, "qewewqewqeqweqw"),
      (3, "{\"Name\":\"Jhon\"}]", null, "56", "vxcxcvvxxv")
    ).toDF("id", "names", "country", "age", "email")


    val result: Either[Throwable, DataFrame] = quality.jsonToComplexType(inputDF)

    val dfResult = result.right.get
    result.isRight shouldBe true

    dfResult.printSchema()
    assert(dfResult.count() == 3)
    assert(dfResult.schema.fields.count(f => f.name == "country" && f.dataType.isInstanceOf[StructType]) == 1)
    assert(dfResult.schema.fields.count(f => f.name == "names" && f.dataType.isInstanceOf[StringType]) == 1)

  }

  test("jsonToComplexType: should return Country json field as StructType and names as ArrayType and skip boolean column ") {

    val inputDF = Seq(
      (1, true ,"[{\"Name\":\"Alice\"},{\"Name\":\"Jhon\"}]", "{  \"Country\": \"Spain\" }", "30", null),
      (2, false,"[{\"Name\":\"Alice\"}]", "{  \"Country\": \"Spain\" }", null, "qewewqewqeqweqw"),
      (3, true,"{\"Name\":\"Jhon\"}]", null, "56", "vxcxcvvxxv")
    ).toDF("id", "high" ,"names", "country", "age", "email")

    val result: Either[Throwable, DataFrame] = quality.jsonToComplexType(inputDF)

    val dfResult = result.right.get
    result.isRight shouldBe true

    dfResult.printSchema()
    assert(dfResult.count() == 3)
    assert(dfResult.schema.fields.count(f => f.name == "country" && f.dataType.isInstanceOf[StructType]) == 1)
    assert(dfResult.schema.fields.count(f => f.name == "names" && f.dataType.isInstanceOf[ArrayType]) == 1)

  }


}
