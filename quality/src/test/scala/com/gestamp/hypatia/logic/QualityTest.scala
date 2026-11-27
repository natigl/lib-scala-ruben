package com.gestamp.hypatia.logic


import com.gestamp.hypatia._
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import java.sql.Date
import java.sql.Timestamp
import java.time.Instant

class QualityTest extends BaseTest {
  import spark.implicits._

  test("Convert from SAP format to date to one column") {

    val schema = StructType(Array(
      StructField("Date", StringType, nullable = true)
    ))

    val qualitySAP = new QualitySAP {}

    val rowData = Seq(Row("79899898"), //2010-01-01
                      Row("79999898"), //2000-01-01
                      Row("79829279")) //2017-07-20

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)

    val dfFormat = qualitySAP.sapDateFormat(df,"Date")
    val actual = dfFormat.right.getOrElse(spark.emptyDataFrame)
    actual.show()

    val expected = Seq(("2010-01-01"), ("2000-01-01"), ("2017-07-20") )
      .toDF("Date")
      .withColumn("Date", to_date(col("Date")))
    expected.show()

    equalDataFrames(actual, expected)
  }

  test("Convert from SAP format to date to more than one column") {

    val schema = StructType(Array(
      StructField("Date1", StringType, nullable = true),
      StructField("Date2", StringType, nullable = true)
    ))

    val qualitySAP = new QualitySAP {}

    val rowData = Seq(Row("79899898", "79899898"), //2010-01-01
      Row("79999898", "79999898"), //2000-01-01
      Row("79829279", "79829279")) //2017-07-20

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)

    val dfFormat = qualitySAP.sapDateFormat(df, "Date1,Date2")
    val actual = dfFormat.right.getOrElse(spark.emptyDataFrame)
    actual.show()

    val expected = Seq(("2010-01-01", "2010-01-01"), ("2000-01-01", "2000-01-01"), ("2017-07-20", "2017-07-20"))
      .toDF("Date1", "Date2")
      .withColumn("Date1", to_date(col("Date1")))
      .withColumn("Date2", to_date(col("Date2")))
    expected.show()

    equalDataFrames(actual, expected)
  }

  test("Convert when match value otherwise") {

    val schema = StructType(Array(
      StructField("ColumnName", StringType, nullable = false)
    ))

    val quality = new QualityDefault {}

    val rowData = Seq(Row("X"),
      Row("randomString"),
      Row("randomString2"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)

    val dfFormat = quality.replaceColumnWhen(df, "ColumnName", """{ "X":"true", "otherwise" : "false" }""")
    val actual = dfFormat.right.getOrElse(spark.emptyDataFrame)
    actual.show()

    val expectedRowData = Seq(Row("true"), Row("false"), Row("false"))
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(expectedRowData), schema)
    expected.show()

    equalDataFrames(actual, expected)
  }

  test("Convert when match multiples values without otherwise") {

    val schema = StructType(Array(
      StructField("ColumnName", StringType, nullable = false)
    ))

    val quality = new QualityDefault {}

    val rowData = Seq(Row("X"),
      Row("Y"),
      Row("3"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)

    val dfFormat = quality.replaceColumnWhen(df, "ColumnName", """{ "X":"1", "Y" : "2" }""")
    val actual = dfFormat.right.getOrElse(spark.emptyDataFrame)
    actual.show()

    val expectedRowData = Seq(Row("1"), Row("2"), Row("3"))
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(expectedRowData), schema)
    expected.show()

    equalDataFrames(actual, expected)
  }

  test("formatDates - Convert date formats") {

    val dfColumns = "dd/MM/yyyy,dd-MM-yyyy,yyyy-MM-dd,yyyy-MM-ddHH:mm:ss,yyyyMMdd,yyyyMMddHHmmss"

    val schema = StructType(Array(
      StructField("dd/MM/yyyy", StringType, nullable = true),
      StructField("dd-MM-yyyy", StringType, nullable = true),
      StructField("yyyy-MM-dd", StringType, nullable = true),
      StructField("yyyy-MM-ddHH:mm:ss", StringType, nullable = true),
      StructField("yyyyMMdd", StringType, nullable = true),
      StructField("yyyyMMddHHmmss", StringType, nullable = true)
    ))

    val qualitySAP = new QualitySAP {}

    val rowData = Seq(
      Row("05/09/2023", "05-09-2023", "2023-09-05", "2023-09-0514:41:05", "20230905", "20230905144105") //2023-09-05T14:41:05.000+0000
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)

    val dfWithFormat = qualitySAP.formatDates(df, dfColumns, "dd/MM/yyyy").right.getOrElse(spark.emptyDataFrame)
      .union(qualitySAP.formatDates(df, dfColumns, "dd-MM-yyyy").right.getOrElse(spark.emptyDataFrame))
      .union(qualitySAP.formatDates(df, dfColumns, "yyyy-MM-dd").right.getOrElse(spark.emptyDataFrame))
      .union(qualitySAP.formatDates(df, dfColumns, "yyyy-MM-ddHH:mm:ss").right.getOrElse(spark.emptyDataFrame))
      .union(qualitySAP.formatDates(df, dfColumns, "yyyyMMdd").right.getOrElse(spark.emptyDataFrame))
      .union(qualitySAP.formatDates(df, dfColumns, "yyyyMMddHHmmss").right.getOrElse(spark.emptyDataFrame))

    val dfWithoutFormat = qualitySAP.formatDates(df, dfColumns).right.getOrElse(spark.emptyDataFrame)

    val dfActual = dfWithFormat.union(dfWithoutFormat)

    dfActual.show(false)

    val expected = Seq(
      ("2023-09-05T00:00:00.000+0000", "", "", "", "", ""),
      ("", "2023-09-05T00:00:00.000+0000", "", "", "", ""),
      ("", "", "2023-09-05T00:00:00.000+0000", "", "", ""),
      ("", "", "", "2023-09-05T14:41:05.000+0000", "", ""),
      ("", "", "", "", "2023-09-05T00:00:00.000+0000", ""),
      ("", "", "", "", "", "2023-09-05T14:41:05.000+0000"),
      ("2023-09-05T00:00:00.000+0000", "2023-09-05T00:00:00.000+0000", "2023-09-05T00:00:00.000+0000", "2023-09-05T14:41:05.000+0000", "2023-09-05T00:00:00.000+0000", "2023-09-05T14:41:05.000+0000")
    ).toDF(dfColumns.split(","):_*)
    expected.show(false)

    equalDataFrames(expected, dfActual)
  }

  test("New Format Dates") {
    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("lastDate", DateType, nullable = true),
      StructField("lastTime", TimestampType, nullable = true)
    ))

    val rowData = Seq(
      Row(1, Date.valueOf("2024-04-01"), Date.valueOf("2025-03-02"), Timestamp.from(Instant.now()))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)

    df.show
    println(df.schema)

    val quality = new QualityDefault()
    val actual = quality.formatDatesAsString(df, "date,lastDate", "yyyy/MM/dd").right.getOrElse(spark.emptyDataFrame)
    val actual2 = quality.formatDatesAsStringNew(actual, "lastTime", "yyyy-MM-dd").right.getOrElse(spark.emptyDataFrame)
    actual2.show
    println(actual2.schema)

    val actual3 = quality.formatDatesAsStringNew(actual, "date,lastDate", "yyyy-MM-dd").right.getOrElse(spark.emptyDataFrame)
    actual3.show
    println(actual3.show)

  }
}



