package com.gestamp.hypatia.logic

import com.gestamp.hypatia.SparkSessionMock
import com.gestamp.hypatia.exceptions.QualityDefaultException
import com.gestamp.hypatia.spark.SparkConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, isnan}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.slf4j.{Logger, LoggerFactory}

class QualityDefaultTest extends FunSuite with Matchers with BeforeAndAfter with SparkSessionMock {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val qualityDefault = new QualityDefault {}

  mockSparkContext()
  val spark = SparkConfiguration.spark

  import spark.implicits._

  val inputDF = Seq(
    (1,"Alice", "Spain", "30", null),
    (2,"Bob", "Spain", null, "qewewqewqeqweqw"),
    (3,"Charlie", null, "56", "vxcxcvvxxv")
  ).toDF("id", "name", "country", "age", "email")

  val inputDF_2 = Seq(
    (1,"Alice", "Spain", "30", null),
    (1,"Alice", "Spain", "30", null),
    (2,"Bob", "Spain", null, "qewewqewqeqweqw"),
    (3,"Charlie", null, "56", "vxcxcvvxxv")
  ).toDF("id", "name", "country", "age", "email")

  val inputDF_dates = Seq(
    (1,"Alice", "Spain", "2021/07/07 00:00:00", "2021/07/06"),
    (2,"Bob", "Spain", "2021/07/07 00:00:00", "2021/07/06"),
    (3,"Charlie", "Holland", "2021/07/07 00:00:00", "2021/07/06")
  ).toDF("id", "name", "country", "birth", "expedition_date")

  val inputDF_dates_with_nulls = Seq(
    (1,"Alice", "Spain", "2021/07/07 00:00:00", null),
    (2,"Bob", "Spain", null, "2021/07/06"),
    (3,"Charlie", "2021/07/07 00:00:00", "2021/07/07 00:00:00", "2021/07/06")
  ).toDF("id", "name", "country", "birth", "expedition_date")

  val inputDF_regex = Seq(
    (1,"Alice", "Spain", "[0FISYEAR].[hola/adios 2023]", null),
    (1,"Alice", "Spain", "[0FISYEAR].[2023]", null),
    (2,"Bob", "Spain", "[0FISYEAR].[hola adios 2023]", "qewewqewqeqweqw"),
    (3,"Charlie", "Holland", "[0FISYEAR].[hola2023]", "vxcxcvvxxv")
  ).toDF("id", "name", "country", "age", "email")

  val inputDF_jsonfield = Seq(
    (1,"Alice", "Spain", "{\"InternalId\":\"WS1534339482\",\"RegistrationStatus\":\"InRegistration\",\"RegistrationUpdateStatus\":\"\"}", null),
    (1,"Alice", "Spain", "{\"InternalId\":\"WS1534339482\",\"RegistrationStatus\":\"InRegistration\",\"RegistrationUpdateStatus\":\"\"}", null),
    (2,"Bob", "Spain", "{\"InternalId\":\"WS1534339482\",\"RegistrationStatus\":\"InRegistration\",\"RegistrationUpdateStatus\":\"\"}", "qewewqewqeqweqw"),
    (3,"Charlie", null, "{\"InternalId\":\"WS1534339482\",\"RegistrationStatus\":\"InRegistration\",\"RegistrationUpdateStatus\":\"\"}", "vxcxcvvxxv")
  ).toDF("id", "name", "country", "json_field", "email")

  val inputDF_structfield = Seq(
    (1,"Alice", "Spain", "{\"InternalId\":\"WS1534339482\",\"RegistrationStatus\":\"InRegistration\",\"RegistrationUpdateStatus\":\"\"}", null),
    (1,"Alice", "Spain", "{\"InternalId\":\"WS1534339482\",\"RegistrationStatus\":\"InRegistration\",\"RegistrationUpdateStatus\":\"\"}", null),
    (2,"Bob", "Spain", "{\"InternalId\":\"WS1534339482\",\"RegistrationStatus\":\"InRegistration\",\"RegistrationUpdateStatus\":\"\"}", "qewewqewqeqweqw"),
    (3,"Charlie", null, "{\"InternalId\":\"WS1534339482\",\"RegistrationStatus\":\"InRegistration\",\"RegistrationUpdateStatus\":\"\"}", "vxcxcvvxxv")
  ).toDF("id", "name", "country", "struct_field", "email")


  test("DropColumns: should return Right(DataFrame) when given column names to be dropped") {

    val result: Either[Throwable, DataFrame] = qualityDefault.dropColumns(inputDF, "age,email")

    result.isRight shouldBe true

    val dfResult = result.right.get
    dfResult.schema.fieldNames should contain("id")
    dfResult.schema.fieldNames should contain("name")
    dfResult.schema.fieldNames should not contain("age")
    dfResult.schema.fieldNames should not contain("email")
  }

  test("DropColumns: should return Left(QualityDefaultException) when given column names contains non-existing " +
    "column names to be dropped") {

    val result: Either[Throwable, DataFrame] = qualityDefault.dropColumns(inputDF, "city,NIF")

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("DropColumns: should return Left(QualityDefaultException) when at least one of the given column names " +
    "doesn't exist in the dataframe") {

    val result: Either[Throwable, DataFrame] = qualityDefault.dropColumns(inputDF, "city,name")

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }


  test("filterEqualDataFrame: returns the filtered dataframe") {
    val columns = "id"
    val value = "2"

    val result = qualityDefault.filterEqualDataFrame(inputDF, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 1)
    assert(dfResult.filter(col("id") === "2").count() == 1)
  }

  test("filterEqualDataFrame: returns an error when filtering") {
    val columns = "city"
    val value = "Madrid"

    val result = qualityDefault.filterEqualDataFrame(inputDF, columns, value)

    assert(result.isLeft)
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterEqualDataFrame: returns an error when a column is not in dataframe") {
    val columns = "id,city"
    val value = "2"

    val result = qualityDefault.filterEqualDataFrame(inputDF, columns, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterNotEqualDataFrame: returns correct rows when the column have nulls") {
    val columns = "age"
    val value = "30"

    val result = qualityDefault.filterNotEqualDataFrame(inputDF, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (2,3 )).count() == 2 )
  }


  test("filterNotEqualDataFrame: returns the filtered dataframe") {
    val columns = "id"
    val value = "2"

    val result = qualityDefault.filterNotEqualDataFrame(inputDF, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") === "2").count() == 0)
  }

  test("filterNotEqualDataFrame: returns an error when filtering") {
    val columns = "city"
    val value = "Madrid"

    val result = qualityDefault.filterNotEqualDataFrame(inputDF, columns, value)

    assert(result.isLeft)
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterNotEqualDataFrame: returns an error when a column is not in dataframe") {
    val columns = "id,city"
    val value = "2"

    val result = qualityDefault.filterNotEqualDataFrame(inputDF, columns, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }



  test("replaceColumnWhen: returns the dataframe with updated columns") {
    val columns = "id,name"
    val newValue = "{\"1\":\"One\", \"2\":\"Two\"}"

    val result = qualityDefault.replaceColumnWhen(inputDF, columns, newValue)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 3)
    assert(dfResult.filter(col("id") === "One").count() == 1)
  }

  test("replaceColumnWhen: throws an IllegalArgumentException when columns is not found in dataframe") {
    val columns = "id,city"
    val newValue = "{\"1\":\"One\", \"2\":\"Two\"}"

    val result = qualityDefault.replaceColumnWhen(inputDF, columns, newValue)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("replaceColumnWhen: returns an error when JSON value cannot be parsed") {
    val columns = "id,name"
    val newValue = "{\"1\":\"One\"\"2\":\"Two\"}" //Falta la ","

    val result = qualityDefault.replaceColumnWhen(inputDF, columns, newValue)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  /*test("replaceColumnWhen: returns an error when column value is not a string") {
    val df = Seq(("A", 1),("B", 2),("C", 3)).toDF("col1", "col2")
    val columns = "id,name"
    val newValue = "{\"1\": 1, \"2\": \"Two\"}"

    val result = qualityDefault.replaceColumnWhen(inputDF, columns, newValue)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }*/


/*  val inputDF = Seq(
    (1,"Alice", "Spain", "30", null),
    (2,"Bob", "Spain", null, "qewewqewqeqweqw"),
    (3,"Charlie", null, "56", "vxcxcvvxxv")
  ).toDF("id", "name", "country", "age", "email")*/


  test("fillNulls: should replace null values in the dataframe with a constant value") {
    val columns = "country"
    val newValue = "New Value"
    val result = qualityDefault.fillNulls(inputDF, columns, newValue)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count == 3)
    assert(dfResult.filter(col("country") === newValue).count == 1)
  }

  test("fillNulls: should replace all null values in the dataframe with a constant value when column value is '*.'") {
    val columns = "*"
    val newValue = "New Value"
    val result = qualityDefault.fillNulls(inputDF, columns, newValue)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count == 3)
    assert(dfResult.filter(col("country").isNull).count == 0)
    assert(dfResult.filter(col("age").isNull).count == 0)
    assert(dfResult.filter(col("email").isNull).count == 0)
  }

  test("fillNulls: should throw an exception when column specified is not found in the dataframe") {
    val columns = "city"
    val newValue = "New Value"
    val result = qualityDefault.fillNulls(inputDF, columns, newValue)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

 /* val inputDF_2 = Seq(
    (1,"Alice", "Spain", "30", null),
    (1,"Alice", "Spain", "30", null),
    (2,"Bob", "Spain", null, "qewewqewqeqweqw"),
    (3,"Charlie", null, "56", "vxcxcvvxxv")
  ).toDF("id", "name", "country", "age", "email")*/


  test("dropDuplicates should return Right dataframe when given valid columns") {
    val columns = "id,name"
    val expectedDf = Seq(
      (1,"Alice", "Spain", "30", null),
      (1,"Alice", "Spain", "30", null),
      (2,"Bob", "Spain", null, "qewewqewqeqweqw"),
      (3,"Charlie", null, "56", "vxcxcvvxxv")
    ).toDF("id", "name", "country", "age", "email")

    val result = qualityDefault.dropDuplicates(inputDF_2, columns)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.except(expectedDf).count() == 0)
  }

  test("dropDuplicates: should drop all rows in the dataframe with a constant value when column value is '*.'") {
    val columns = "*"
    val expectedDf = Seq(
      (1,"Alice", "Spain", "30", null),
      (2,"Bob", "Spain", null, "qewewqewqeqweqw"),
      (3,"Charlie", null, "56", "vxcxcvvxxv")
    ).toDF("id", "name", "country", "age", "email")

    val result = qualityDefault.dropDuplicates(inputDF_2, columns)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.except(expectedDf).count() == 0)
  }

  test("dropDuplicates should return Left when given invalid columns") {
    val columns = "city"

    val result = qualityDefault.dropDuplicates(inputDF_2, columns)
    result.isLeft shouldBe true

    val dfResult = result.left.get
    assert(dfResult.isInstanceOf[QualityDefaultException])
  }



  test("formatDates: should return Left if a column does not exist") {
    val columns = "birth,invalid_column"

    val result = qualityDefault.formatDates(inputDF_dates, columns)

    result.isLeft shouldBe true

    val dfResult = result.left.get
    assert(dfResult.isInstanceOf[QualityDefaultException])
  }

  test("formatDates: should format date columns correctly") {
    //val df = Seq((1, "2021/07/06", "2021/07/07 00:00:00"), (2, "2021/07/07", "2021/07/08 00:00:00")).toDF("id", "date_str", "datetime_str")
    val columns = "birth,expedition_date"
    //val expectedDf = Seq((1, "2021-07-06T00:00:00.000+0000", "2021-07-07T00:00:00.000+0000"), (2, "2021-07-07T00:00:00.000+0000", "2021-07-08T00:00:00.000+0000")).toDF("id", "date_str", "datetime_str")
    val expectedDf = Seq(
      (1,"Alice", "Spain", "2021-07-07T00:00:00.000+0000", "2021-07-06T00:00:00.000+0000"),
      (2,"Bob", "Spain", "2021-07-07T00:00:00.000+0000", "2021-07-06T00:00:00.000+0000"),
      (3,"Charlie", "Holland", "2021-07-07T00:00:00.000+0000", "2021-07-06T00:00:00.000+0000")
    ).toDF("id", "name", "country", "birth", "expedition_date")

    val result = qualityDefault.formatDates(inputDF_dates, columns)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.except(expectedDf).count() == 3)
  }





  test("regexp: should update the DataFrame successfully for valid columns") {
    val columns = "age"
    val pattern = """^\[[^\]]+\]\.\[([^\]]+)\]"""
    val replacement = "$1"
    val expectedDF = Seq(
      (1,"Alice", "Spain", "hola/adios 2023", null),
      (1,"Alice", "Spain", "2023", null),
      (2,"Bob", "Spain", "hola adios 2023", "qewewqewqeqweqw"),
      (3,"Charlie", "Holland", "hola2023", "vxcxcvvxxv")
    ).toDF("id", "name", "country", "age", "email")

    val result = qualityDefault.regexp(inputDF_regex, columns, pattern, replacement)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.schema === expectedDF.schema)
    assert(dfResult.collectAsList() === expectedDF.collectAsList())
  }

  test("regexp: should return an error for invalid columns") {
    val columns = "city"
    val pattern = """^\[[^\]]+\]\.\[([^\]]+)\]"""
    val replacement = "$1"

    val result = qualityDefault.regexp(inputDF_regex, columns, pattern, replacement)

    result.isLeft shouldBe true

    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("regexp should return an error when an exception is thrown") {
    val columns = "id"
    val pattern = ""
    val replacement = ""

    val result = qualityDefault.regexp(inputDF_regex, columns, pattern, replacement)

    result.isLeft shouldBe true

    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }




  test("flattenJsonField: should return a DataFrame with correct schema and values") {
    val result = qualityDefault.flattenJsonField(inputDF_jsonfield, "json_field")

    result.isRight shouldBe true

    val explodedDF = result.right.get
    assert(explodedDF.schema == StructType(
      StructField("id", IntegerType, false) :: StructField("name", StringType, true) :: StructField("country", StringType, true)
        :: StructField("email", StringType, true)  :: StructField("InternalId", StringType, true) :: StructField("RegistrationStatus", StringType, true)
        :: StructField("RegistrationUpdateStatus", StringType, true)  :: Nil
    ))
  }

  test("flattenJsonField: should return a QualityDefaultException because the field is not a JSON Strng") {
    val result = qualityDefault.flattenJsonField(inputDF_jsonfield, "id")

    result.isLeft shouldBe true

    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }





  test("flattenStructField: should return a DataFrame with correct schema and values") {
    val structFieldSchema = StructType(
      StructField("InternalId", StringType, true) :: StructField("RegistrationStatus", StringType, true)
        :: StructField("RegistrationUpdateStatus", StringType, true) :: Nil
    )

    val inputDF_withStructType = inputDF_structfield.withColumn("struct_field", from_json(col("struct_field"), structFieldSchema))

    val result = qualityDefault.flattenStructField(inputDF_withStructType, "struct_field")

    val explodedDF = result.right.get
    assert(explodedDF.schema == StructType(
      StructField("id", IntegerType, false) :: StructField("name", StringType, true) :: StructField("country", StringType, true)
        :: StructField("email", StringType, true)  :: StructField("InternalId", StringType, true) :: StructField("RegistrationStatus", StringType, true)
        :: StructField("RegistrationUpdateStatus", StringType, true) :: Nil
    ))
  }

  test("flattenStructField: should return a QualityDefaultException on invalid field type") {
    val structFieldSchema = StructType(
      StructField("InternalId", StringType, true) :: StructField("RegistrationStatus", StringType, true)
        :: StructField("RegistrationUpdateStatus", StringType, true) :: Nil
    )
    val inputDF_withStructType = inputDF_structfield.withColumn("struct_field", from_json(col("struct_field"), structFieldSchema))

    val result = qualityDefault.flattenStructField(inputDF_withStructType, "email")

    result.isLeft shouldBe true

    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }




  val inputDF_range = Seq[(Int, String, Option[Double], Option[Double])](
    (1, "Alice", Some(100.0), Some(200.0)),
    (2, "Bob", Some(5000.0), Some(4999.0)),
    (3, "Charlie", Some(50.0), Some(300.0)),
    (4, "Diana", Some(6000.0), Some(300.0)),
    (5, "Eve", None, Some(300.0)),
    (6, "Frank", Some(300.0), Some(9000.0))
  ).toDF("id", "name", "weight", "length")

  val inputDF_range_string = Seq(
    (1, "Alice", "150"),
    (2, "Bob", "abc"),
    (3, "Charlie", "20"),
    (4, "Diana", null)
  ).toDF("id", "name", "age")

  val inputDF_range_non_finite = Seq[(Int, String, Double)](
    (1, "Alice", 150.0),
    (2, "Bob", Double.NaN),
    (3, "Charlie", Double.PositiveInfinity),
    (4, "Diana", Double.NegativeInfinity),
    (5, "Eve", 4000.0)
  ).toDF("id", "name", "weight")

  val inputDF_range_string_non_finite = Seq(
    (1, "Alice", "150"),
    (2, "Bob", "NaN"),
    (3, "Charlie", "Infinity"),
    (4, "Diana", "-Infinity"),
    (5, "Eve", "4000")
  ).toDF("id", "name", "age")


  test("filterRangeDataFrame: keeps the rows inside the closed range and discards the ones outside it") {
    val columns = "weight"
    val value = "100,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 3)
    assert(dfResult.filter(col("id") isin (1, 2, 6)).count() == 3)
    assert(dfResult.filter(col("id") isin (3, 4, 5)).count() == 0)
  }

  test("filterRangeDataFrame: keeps the rows whose value is exactly one of the bounds because the range is inclusive") {
    val columns = "weight"
    val value = "100,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.filter(col("weight") === 100.0).count() == 1)
    assert(dfResult.filter(col("weight") === 5000.0).count() == 1)
  }

  test("filterRangeDataFrame: keeps every row over the minimum when the range has no maximum bound") {
    val columns = "weight"
    val value = "100,"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 4)
    assert(dfResult.filter(col("id") isin (1, 2, 4, 6)).count() == 4)
    assert(dfResult.filter(col("id") === 3).count() == 0)
  }

  test("filterRangeDataFrame: keeps every row under the maximum when the range has no minimum bound") {
    val columns = "weight"
    val value = ",5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 4)
    assert(dfResult.filter(col("id") isin (1, 2, 3, 6)).count() == 4)
    assert(dfResult.filter(col("id") === 4).count() == 0)
  }

  test("filterRangeDataFrame: keeps a row only when all the given columns are inside the range") {
    val columns = "weight,length"
    val value = "100,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 2)).count() == 2)
    // Frank is inside the range for weight (300) but outside it for length (9000): AND semantics discards the row
    assert(dfResult.filter(col("id") === 6).count() == 0)
  }

  test("filterRangeDataFrame: compares as a number the values of a column read as string") {
    val columns = "age"
    val value = "100,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range_string, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 1)
    assert(dfResult.filter(col("age") === "150").count() == 1)
  }

  test("filterRangeDataFrame: discards the rows with a null value in one of the checked columns") {
    val columns = "weight"
    val value = "0,10000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 5)
    assert(dfResult.filter(col("id") === 5).count() == 0)
  }

  test("filterRangeDataFrame: discards without failing the rows whose value cannot be casted to a number") {
    val columns = "age"
    val value = "0,10000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range_string, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (2, 4)).count() == 0)
  }

  test("filterRangeDataFrame: returns an error when the range does not have exactly two bounds") {
    val columns = "weight"

    val resultOneBound = qualityDefault.filterRangeDataFrame(inputDF_range, columns, "100")
    val resultThreeBounds = qualityDefault.filterRangeDataFrame(inputDF_range, columns, "1,2,3")

    resultOneBound.isLeft shouldBe true
    assert(resultOneBound.left.get.isInstanceOf[QualityDefaultException])

    resultThreeBounds.isLeft shouldBe true
    assert(resultThreeBounds.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterRangeDataFrame: returns an error when both bounds of the range are empty") {
    val columns = "weight"
    val value = ","

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterRangeDataFrame: returns an error when one of the bounds of the range is not a number") {
    val columns = "weight"
    val value = "abc,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterRangeDataFrame: returns an error when the minimum bound is greater than the maximum one") {
    val columns = "weight"
    val value = "5000,100"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterRangeDataFrame: returns an error when a column is not in dataframe") {
    val columns = "weight,city"
    val value = "100,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterRangeDataFrame: discards the rows whose value is NaN when the range has no maximum bound") {
    val columns = "weight"
    val value = "100,"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range_non_finite, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    // For Spark NaN is greater than any other value and it is not null, so without an explicit finiteness
    // check the row would survive an open range like "100,"
    assert(dfResult.filter(col("id") === 2).count() == 0)
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 5)).count() == 2)
  }

  test("filterRangeDataFrame: discards the rows whose value is Infinity when the range has no maximum bound") {
    val columns = "weight"
    val value = "100,"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range_non_finite, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    // Infinity is not caught by isnan: it is discarded by the comparison against +Infinity
    assert(dfResult.filter(col("id") === 3).count() == 0)
    assert(dfResult.filter(col("id") isin (1, 5)).count() == 2)
  }

  test("filterRangeDataFrame: discards the rows whose value is -Infinity when the range has no minimum bound") {
    val columns = "weight"
    val value = ",5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range_non_finite, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    // -Infinity is lower than the maximum bound, so only the comparison against -Infinity discards it
    assert(dfResult.filter(col("id") === 4).count() == 0)
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 5)).count() == 2)
  }

  test("filterRangeDataFrame: discards the rows whose value is the text NaN when the range has no maximum bound") {
    val columns = "age"
    val value = "100,"

    // A column read from a CSV keeps "NaN" as text and the cast to DoubleType turns it into a real NaN,
    // not into a null, so checking only isNotNull would not discard the row
    assert(inputDF_range_string_non_finite.filter(isnan(col("age").cast(DoubleType))).count() == 1)

    val result = qualityDefault.filterRangeDataFrame(inputDF_range_string_non_finite, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.filter(col("id") === 2).count() == 0)
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 5)).count() == 2)
  }

  test("filterRangeDataFrame: discards the rows whose value is the text Infinity when the range has no maximum bound") {
    val columns = "age"
    val value = "100,"

    assert(inputDF_range_string_non_finite.filter(col("age").cast(DoubleType) === Double.PositiveInfinity).count() == 1)

    val result = qualityDefault.filterRangeDataFrame(inputDF_range_string_non_finite, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.filter(col("id") === 3).count() == 0)
    assert(dfResult.filter(col("id") isin (1, 5)).count() == 2)
  }

  test("filterRangeDataFrame: discards the rows whose value is the text -Infinity when the range has no minimum bound") {
    val columns = "age"
    val value = ",5000"

    assert(inputDF_range_string_non_finite.filter(col("age").cast(DoubleType) === Double.NegativeInfinity).count() == 1)

    val result = qualityDefault.filterRangeDataFrame(inputDF_range_string_non_finite, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.filter(col("id") === 4).count() == 0)
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 5)).count() == 2)
  }

  test("filterRangeDataFrame: returns an error when one of the bounds of the range is not a finite number") {
    val columns = "weight"

    // "NaN", "Infinity" and "-Infinity" are valid literals for toDouble: without an explicit check the rule
    // would return an empty dataframe instead of failing, and the destination table would be overwritten empty
    val resultNaN = qualityDefault.filterRangeDataFrame(inputDF_range, columns, "NaN,5000")
    val resultInfinity = qualityDefault.filterRangeDataFrame(inputDF_range, columns, "100,Infinity")
    val resultNegativeInfinity = qualityDefault.filterRangeDataFrame(inputDF_range, columns, "-Infinity,5000")

    resultNaN.isLeft shouldBe true
    assert(resultNaN.left.get.isInstanceOf[QualityDefaultException])

    resultInfinity.isLeft shouldBe true
    assert(resultInfinity.left.get.isInstanceOf[QualityDefaultException])

    resultNegativeInfinity.isLeft shouldBe true
    assert(resultNegativeInfinity.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterRangeDataFrame: reports a non finite bound and a bound that is not a number as different errors") {
    val columns = "weight"

    val resultNonFinite = qualityDefault.filterRangeDataFrame(inputDF_range, columns, "NaN,5000")
    val resultNotANumber = qualityDefault.filterRangeDataFrame(inputDF_range, columns, "abc,5000")

    resultNonFinite.isLeft shouldBe true
    resultNonFinite.left.get.getMessage should include("is not a finite number")

    resultNotANumber.isLeft shouldBe true
    resultNotANumber.left.get.getMessage should include("is not a number")
    resultNotANumber.left.get.getMessage should not include "is not a finite number"
  }

  test("filterRangeDataFrame: returns an error when the columns parameter is null") {
    val value = "100,5000"

    // metadata.quality.fields is nullable: a null must be a configuration error, not a NullPointerException
    val result = qualityDefault.filterRangeDataFrame(inputDF_range, null, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterRangeDataFrame: returns an error when the value parameter is null") {
    val columns = "weight"

    // metadata.quality.value is nullable: a null must be a configuration error, not a NullPointerException
    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, null)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }


  test("filterRangeDataFrame: reports a null columns parameter as a configuration error of the rule") {
    val value = "100,5000"

    // metadata.quality.fields is nullable: a null must be reported as an invalid configuration of the rule,
    // not as a column missing in the DataFrame and even less as an unintelligible analysis error over a
    // column named "". Asserting only the type would not tell the two apart
    val result = qualityDefault.filterRangeDataFrame(inputDF_range, null, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
    result.left.get.getMessage should include("at least one column name")
  }

  test("filterRangeDataFrame: returns an error when the columns parameter is empty") {
    val value = "100,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, "", value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
    // An empty columns is a configuration error of the rule, not a column that the DataFrame does not have
    result.left.get.getMessage should include("at least one column name")
  }

  test("filterRangeDataFrame: returns an error when the columns parameter only holds a comma") {
    val value = "100,5000"

    // "," leaves no column name once the empty parts are discarded, so it is the same configuration error
    // as an empty columns, not a condition built over a column named ""
    val result = qualityDefault.filterRangeDataFrame(inputDF_range, ",", value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
    result.left.get.getMessage should include("at least one column name")
  }

  test("filterRangeDataFrame: returns an error when the columns parameter only holds spaces around a comma") {
    val value = "100,5000"

    // The parts are trimmed before checking that at least one column name is left, so " , " is as empty as ","
    val result = qualityDefault.filterRangeDataFrame(inputDF_range, " , ", value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
    result.left.get.getMessage should include("at least one column name")
  }

  test("filterRangeDataFrame: tolerates a trailing comma in the columns parameter") {
    val columns = "weight,"
    val value = "100,5000"

    // The empty column names that the split leaves are discarded, so the rule applies to the columns
    // actually written
    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 3)
    assert(dfResult.filter(col("id") isin (1, 2, 6)).count() == 3)
  }

  test("filterRangeDataFrame: tolerates a leading comma in the columns parameter") {
    val columns = ",weight"
    val value = "100,5000"

    // Both commas are tolerated: a leading comma behaves like a trailing one, so there is no asymmetry
    // between ",weight" and "weight,"
    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 3)
    assert(dfResult.filter(col("id") isin (1, 2, 6)).count() == 3)
  }

  test("filterRangeDataFrame: filters the same rows with a leading comma, a trailing comma and no comma at all") {
    val value = "100,5000"

    val resultPlain = qualityDefault.filterRangeDataFrame(inputDF_range, "weight", value)
    val resultLeading = qualityDefault.filterRangeDataFrame(inputDF_range, ",weight", value)
    val resultTrailing = qualityDefault.filterRangeDataFrame(inputDF_range, "weight,", value)

    resultPlain.isRight shouldBe true
    resultLeading.isRight shouldBe true
    resultTrailing.isRight shouldBe true

    // A stray comma written in metadata.quality.fields must not change which rows the rule keeps
    val idsPlain = resultPlain.right.get.select("id").collect().map(_.getInt(0)).toSet
    val idsLeading = resultLeading.right.get.select("id").collect().map(_.getInt(0)).toSet
    val idsTrailing = resultTrailing.right.get.select("id").collect().map(_.getInt(0)).toSet
    idsLeading shouldBe idsPlain
    idsTrailing shouldBe idsPlain
  }

  test("filterRangeDataFrame: trims the spaces around the names of the columns") {
    val columns = " weight , length "
    val value = "100,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    // The AND semantics is kept after trimming: only the rows with both columns inside the range survive
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 2)).count() == 2)
  }

  test("filterRangeDataFrame: names the configured columns and the missing one in the error message") {
    val columns = "weight,city"
    val value = "100,5000"

    val result = qualityDefault.filterRangeDataFrame(inputDF_range, columns, value)

    result.isLeft shouldBe true

    // In an unattended Databricks job the message is the only diagnosis available: it has to say both what
    // was configured and which column of it is missing, otherwise it is not actionable
    val message = result.left.get.getMessage
    message should include("(weight,city)")
    message should include("not found in the DataFrame: city")
  }


  val inputDF_not_null = Seq[(Int, String, Option[String], Option[Double])](
    (1, "Alice", Some("Spain"), Some(30.0)),
    (2, "Bob", None, Some(45.0)),
    (3, "Charlie", Some("Holland"), None),
    (4, "Diana", None, None),
    (5, "Eve", Some("Spain"), Some(20.0))
  ).toDF("id", "name", "country", "score")

  val inputDF_not_null_empty_string = Seq(
    (1, "Alice", "Spain"),
    (2, "Bob", ""),
    (3, "Charlie", null)
  ).toDF("id", "name", "country")

  val inputDF_not_null_mixed_types = Seq[(Int, String, Option[Long], Option[Double], Option[Boolean])](
    (1, "Alice", Some(0L), Some(0.0), Some(false)),
    (2, "", Some(10L), Some(1.5), Some(true)),
    (3, "Charlie", None, Some(2.5), Some(true))
  ).toDF("id", "name", "size_bytes", "score", "valid")

  val inputDF_not_null_images = Seq[(String, String, Long, Int, Option[Double], Option[Double], Option[Double], Option[Double])](
    ("weld_001.png", "PNG", 154238L, 3, Some(118.4), Some(42.7), Some(7.21), Some(315.6)),
    ("weld_002.png", "PNG", 98211L, 3, Some(102.9), Some(38.1), Some(6.84), Some(210.3)),
    ("weld_corrupted.png", "PNG", 512L, 3, None, None, None, None)
  ).toDF("file_name", "file_format", "blob_size_bytes", "num_channels",
    "pixel_mean", "pixel_std", "histogram_entropy", "sharpness_laplacian_var")

  val inputDF_not_null_nan = Seq[(Int, Option[Double])](
    (1, Some(1.0)),
    (2, Some(Double.NaN)),
    (3, None)
  ).toDF("id", "score")

  // The landing of these datasets is a CSV written with the csv.DictWriter of Python and read by the Auto
  // Loader without inferColumnTypes, so every column arrives as a string: a Python None is an empty field
  // that Spark reads as null, while a float('nan') is written as the text "nan"
  val inputDF_not_null_text_markers = Seq[(String, String)](
    ("weld_001.png", "7.21"),
    ("weld_002.png", "nan"),
    ("weld_003.png", null)
  ).toDF("file_name", "histogram_entropy")


  test("filterNotNullDataFrame: keeps every row when the checked column has no nulls") {
    val columns = "name"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == inputDF_not_null.count())
    assert(dfResult.filter(col("id") isin (1, 2, 3, 4, 5)).count() == 5)
  }

  test("filterNotNullDataFrame: discards only the rows with a null in the checked column") {
    val columns = "country"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 3)
    assert(dfResult.filter(col("id") isin (1, 3, 5)).count() == 3)
    assert(dfResult.filter(col("id") isin (2, 4)).count() == 0)
  }

  test("filterNotNullDataFrame: discards a row with a null in a single one of the checked columns") {
    val columns = "country,score"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 5)).count() == 2)
    // Charlie has country but no score and Bob has score but no country: AND semantics discards both rows,
    // an OR rule like FILTER would have kept them
    assert(dfResult.filter(col("id") isin (2, 3)).count() == 0)
  }

  test("filterNotNullDataFrame: keeps a row whose checked column holds an empty string because it is not null") {
    val columns = "country"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null_empty_string, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("country") === "").count() == 1)
    assert(dfResult.filter(col("id") === 3).count() == 0)
  }

  test("filterNotNullDataFrame: ignores the value parameter, so a null and an empty value give the same result") {
    val columns = "country,score"

    // metadata.quality.value is nullable and this rule needs no configuration: neither a null, an empty
    // value nor an informed one must change the result or raise a NullPointerException
    val resultNullValue = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, null)
    val resultEmptyValue = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, "")
    val resultInformedValue = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, "whatever")

    resultNullValue.isRight shouldBe true
    resultEmptyValue.isRight shouldBe true
    resultInformedValue.isRight shouldBe true

    assert(resultNullValue.right.get.count() == 2)
    assert(resultEmptyValue.right.get.count() == 2)
    assert(resultInformedValue.right.get.count() == 2)
  }

  test("filterNotNullDataFrame: returns an error when a column is not in dataframe") {
    val columns = "country,unknown_column"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
  }

  test("filterNotNullDataFrame: returns an error when the columns parameter is null") {
    val value = ""

    // metadata.quality.fields is nullable: a null must be a configuration error, not a NullPointerException
    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, null, value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
    // Without this assertion the test would still pass if the rule failed for any other reason, for
    // instance because the empty condition was never built
    result.left.get.getMessage should include("at least one column name")
  }

  test("filterNotNullDataFrame: returns an error when the columns parameter is empty") {
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, "", value)

    result.isLeft shouldBe true
    assert(result.left.get.isInstanceOf[QualityDefaultException])
    // An empty columns is a configuration error of the rule, not a column that the DataFrame does not have
    result.left.get.getMessage should include("at least one column name")
  }

  test("filterNotNullDataFrame: discards the metadata of a corrupted image whose measures could not be computed") {
    val columns = "pixel_mean,pixel_std,histogram_entropy,sharpness_laplacian_var"
    val value = ""

    // The corrupted image arrives with the columns the extractor always fills (format, size, channels)
    // correctly informed, so only the null measures identify it as an incomplete row
    assert(inputDF_not_null_images.filter(col("file_name") === "weld_corrupted.png")
      .filter(col("file_format").isNotNull && col("blob_size_bytes").isNotNull && col("num_channels").isNotNull)
      .count() == 1)

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null_images, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("file_name") isin ("weld_001.png", "weld_002.png")).count() == 2)
    assert(dfResult.filter(col("file_name") === "weld_corrupted.png").count() == 0)
  }

  test("filterNotNullDataFrame: checks columns of different types in the same rule without casting their values") {
    val columns = "name,size_bytes,score,valid"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null_mixed_types, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    // 0, 0.0, false and "" are values, not nulls: only the row with a null size_bytes is discarded
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 2)).count() == 2)
    assert(dfResult.filter(col("id") === 3).count() == 0)
  }

  test("filterNotNullDataFrame: trims the spaces around the names of the columns") {
    val columns = " country , score "
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isRight shouldBe true
    assert(result.right.get.count() == 2)
  }

  test("filterNotNullDataFrame: tolerates a trailing comma in the columns parameter") {
    val columns = "country,"
    val value = ""

    // split drops the trailing empty strings, so a trailing comma leaves no empty column name and the rule
    // applies to the columns actually written
    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isRight shouldBe true
    assert(result.right.get.count() == 3)
  }

  test("filterNotNullDataFrame: tolerates a leading comma in the columns parameter") {
    val columns = ",country"
    val value = ""

    // Both commas are tolerated: the empty column names that the split leaves are discarded, so the rule
    // applies to the columns actually written and a leading comma behaves like a trailing one
    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isRight shouldBe true
    assert(result.right.get.count() == 3)
  }

  test("filterNotNullDataFrame: names the configured columns and the missing one in the error message") {
    val columns = "country,unknown_column"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isLeft shouldBe true

    // In an unattended Databricks job the message is the only diagnosis available: it has to say both what
    // was configured and which column of it is missing, otherwise it is not actionable
    val message = result.left.get.getMessage
    message should include("(country,unknown_column)")
    message should include("not found in the DataFrame: unknown_column")
  }

  test("filterNotNullDataFrame: keeps a row whose checked column holds a NaN because NaN is not null") {
    val columns = "score"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null_nan, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    // The rule checks SQL nulls only: it guarantees that the measure is PRESENT, not that it is usable, so
    // the NaN row survives. To discard it too, combine the rule with FILTER_RANGE, which casts to
    // DoubleType and excludes the non finite values
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("id") isin (1, 2)).count() == 2)
    assert(dfResult.filter(isnan(col("score"))).count() == 1)
    assert(dfResult.filter(col("id") === 3).count() == 0)
  }

  test("filterNotNullDataFrame: discards the null of a string column but keeps the textual marker nan") {
    val columns = "histogram_entropy"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null_text_markers, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    // The Python None arrives as null and the rule catches it, but the float('nan') arrives as the string
    // "nan", a perfectly non null value that passes the check unnoticed
    assert(dfResult.count() == 2)
    assert(dfResult.filter(col("histogram_entropy") === "nan").count() == 1)
    assert(dfResult.filter(col("file_name") === "weld_003.png").count() == 0)
  }

  test("filterNotNullDataFrame: returns an empty dataframe with the same schema when the input is empty") {
    val columns = "country,score"
    val value = ""
    val emptyDF = inputDF_not_null.filter(col("id") === -1)

    val result = qualityDefault.filterNotNullDataFrame(emptyDF, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 0)
    dfResult.schema should equal(inputDF_not_null.schema)
  }

  test("filterNotNullDataFrame: returns an empty dataframe when every row has a null in a checked column") {
    val columns = "country,score"
    val value = ""
    val dfOnlyNulls = inputDF_not_null.filter(col("id") isin (2, 3, 4))

    // Discarding every row is a Right with zero rows, not an error: a caller writing the result in FULL mode
    // would overwrite the destination table empty with the job in green
    val result = qualityDefault.filterNotNullDataFrame(dfOnlyNulls, columns, value)

    result.isRight shouldBe true
    assert(result.right.get.count() == 0)
  }

  test("filterNotNullDataFrame: does not modify the columns of the dataframe it filters") {
    val columns = "country"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isRight shouldBe true
    result.right.get.schema should equal(inputDF_not_null.schema)
  }

  test("filterNotNullDataFrame: applies the rule once when the same column is repeated") {
    val columns = "country,country"
    val value = ""

    val result = qualityDefault.filterNotNullDataFrame(inputDF_not_null, columns, value)

    result.isRight shouldBe true

    val dfResult = result.right.get
    assert(dfResult.count() == 3)
    assert(dfResult.filter(col("id") isin (1, 3, 5)).count() == 3)
  }

}
