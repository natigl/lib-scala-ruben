package com.gestamp.hypatia.defs

import com.gestamp.hypatia.utils.CommonsConstants
import org.apache.spark.sql.SaveMode
import org.scalatest.{FunSuite, Matchers}

class HypatiaSparkOptionsTest extends FunSuite with Matchers {

  test("appendTable uses Delta format, Append mode and no overwriteSchema") {
    val options = HypatiaSparkOptions.appendTable()

    options.fileFormat shouldBe CommonsConstants.DELTA_FORMAT
    options.saveMode shouldBe SaveMode.Append
    options.partitionBy shouldBe empty
    options.otherOptions shouldBe empty
  }

  test("appendTable with mergeSchema adds the mergeSchema option") {
    HypatiaSparkOptions.appendTable(mergeSchema = true).otherOptions shouldBe Map("mergeSchema" -> "true")
  }

  test("appendPartitionTable sets Append mode and partition columns") {
    val options = HypatiaSparkOptions.appendPartitionTable(List("a", "b"))

    options.saveMode shouldBe SaveMode.Append
    options.partitionBy shouldBe List("a", "b")
    options.otherOptions shouldBe empty
  }

  test("appendPartitionTable with mergeSchema adds the mergeSchema option") {
    HypatiaSparkOptions.appendPartitionTable(List("a"), mergeSchema = true).otherOptions shouldBe Map("mergeSchema" -> "true")
  }

  test("fullTable and partitionTable keep Overwrite mode with overwriteSchema") {
    HypatiaSparkOptions.fullTable.saveMode shouldBe SaveMode.Overwrite
    HypatiaSparkOptions.fullTable.otherOptions shouldBe Map("overwriteSchema" -> "true")

    val partitioned = HypatiaSparkOptions.partitionTable(List("a"))
    partitioned.saveMode shouldBe SaveMode.Overwrite
    partitioned.partitionBy shouldBe List("a")
    partitioned.otherOptions shouldBe Map("overwriteSchema" -> "true")
  }
}
