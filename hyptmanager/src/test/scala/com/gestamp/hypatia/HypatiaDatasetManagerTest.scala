package com.gestamp.hypatia

import com.gestamp.hypatia.exceptions.HypatiaManagerException
import com.gestamp.hypatia.spark.SparkOptions
import org.apache.spark.sql.SaveMode
import org.scalatest.{FunSuite, Matchers}

class HypatiaDatasetManagerTest extends FunSuite with Matchers {

  test("Overwrite mode is accepted") {
    noException should be thrownBy HypatiaDatasetManager.validateSaveMode(SaveMode.Overwrite)
  }

  test("Append mode is accepted") {
    noException should be thrownBy HypatiaDatasetManager.validateSaveMode(SaveMode.Append)
  }

  test("Other save modes are rejected") {
    an[HypatiaManagerException] should be thrownBy HypatiaDatasetManager.validateSaveMode(SaveMode.ErrorIfExists)
    an[HypatiaManagerException] should be thrownBy HypatiaDatasetManager.validateSaveMode(SaveMode.Ignore)
  }

  private val baseOptions = SparkOptions(fileFormat = "delta", saveMode = SaveMode.Overwrite, outputMode = "complete",
    partitionBy = List("year", "month"), otherOptions = Map("mergeSchema" -> "true"))

  test("withOverwriteSchema adds overwriteSchema=true in Overwrite mode when the option is missing") {
    HypatiaDatasetManager.withOverwriteSchema(baseOptions).otherOptions shouldBe
      Map("mergeSchema" -> "true", "overwriteSchema" -> "true")
  }

  test("withOverwriteSchema keeps the rest of the SparkOptions fields untouched") {
    val result = HypatiaDatasetManager.withOverwriteSchema(baseOptions)

    result shouldBe baseOptions.copy(otherOptions = baseOptions.otherOptions + ("overwriteSchema" -> "true"))
    result.fileFormat shouldBe "delta"
    result.saveMode shouldBe SaveMode.Overwrite
    result.outputMode shouldBe "complete"
    result.partitionBy shouldBe List("year", "month")
  }

  test("withOverwriteSchema leaves an explicit overwriteSchema=true as it is") {
    val options = baseOptions.copy(otherOptions = Map("mergeSchema" -> "true", "overwriteSchema" -> "true"))

    HypatiaDatasetManager.withOverwriteSchema(options) shouldBe options
  }

  test("withOverwriteSchema respects an explicit overwriteSchema=false in Overwrite mode") {
    val options = baseOptions.copy(otherOptions = Map("mergeSchema" -> "true", "overwriteSchema" -> "false"))

    HypatiaDatasetManager.withOverwriteSchema(options) shouldBe options
  }

  test("withOverwriteSchema respects OverwriteSchema=false with another capitalization and does not duplicate it") {
    val options = baseOptions.copy(otherOptions = Map("mergeSchema" -> "true", "OverwriteSchema" -> "false"))

    val result = HypatiaDatasetManager.withOverwriteSchema(options)

    result shouldBe options
    result.otherOptions.keys.count(_.equalsIgnoreCase("overwriteSchema")) shouldBe 1
  }

  test("withOverwriteSchema replaces a null overwriteSchema value with true") {
    val options = baseOptions.copy(otherOptions = Map("mergeSchema" -> "true", "OVERWRITESCHEMA" -> null))

    HypatiaDatasetManager.withOverwriteSchema(options).otherOptions shouldBe
      Map("mergeSchema" -> "true", "overwriteSchema" -> "true")
  }

  test("withOverwriteSchema does not change the options in Append mode") {
    val append = baseOptions.copy(saveMode = SaveMode.Append)
    val appendWithOption = append.copy(otherOptions = Map("overwriteSchema" -> "true"))

    HypatiaDatasetManager.withOverwriteSchema(append) shouldBe append
    HypatiaDatasetManager.withOverwriteSchema(appendWithOption) shouldBe appendWithOption
  }

  test("withOverwriteSchema adds the option to empty otherOptions") {
    // fileFormat explicit: its default (CommonsConstants) initializes DatabricksManager, which needs DBUtils
    val options = SparkOptions(fileFormat = "delta", saveMode = SaveMode.Overwrite, outputMode = "",
      partitionBy = List.empty, otherOptions = Map.empty)
    HypatiaDatasetManager.withOverwriteSchema(options).otherOptions shouldBe
      Map("overwriteSchema" -> "true")
  }
}
