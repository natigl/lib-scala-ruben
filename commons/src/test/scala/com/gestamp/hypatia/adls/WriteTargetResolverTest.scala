package com.gestamp.hypatia.adls

import com.gestamp.hypatia.exceptions.ADLS2ManagerException
import org.scalatest.{FunSuite, Matchers}

/**
 * Pure tests (no Spark) for [[WriteTargetResolver]]: decides whether `ADLS2Manager.sparkWriteData`
 * writes with the `path` option (new table) or by table name (existing table in the same location).
 */
class WriteTargetResolverTest extends FunSuite with Matchers {

  private val TableName = "gold_pre.usa_prodseries_migration_project.material_master"
  private val GoldPath =
    "abfss://application@stgoldhypatiapreneu.dfs.core.windows.net/usa_prodseries_migration_project/material_master"

  private def leftMessage(result: Either[ADLS2ManagerException, Option[String]]): String = result match {
    case Left(e) => e.getMessage
    case Right(v) => fail(s"Expected Left but got Right($v)")
  }

  // ---------------------------------------------------------------------------------------------
  // resolveWriteTarget
  // ---------------------------------------------------------------------------------------------

  test("a table that does not exist is written with the path option") {
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = false, None, GoldPath) shouldBe Right(Some(GoldPath))
  }

  test("a table that does not exist ignores any existing location and keeps the requested path untouched") {
    val requested = GoldPath + "/"
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = false, Some("abfss://other@x.net/y"), requested) shouldBe
      Right(Some(requested))
  }

  test("an existing table whose location is identical to the path is written by name") {
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(GoldPath), GoldPath) shouldBe Right(None)
  }

  test("an existing table is written by name when the location has a trailing slash and the path does not") {
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(GoldPath + "/"), GoldPath) shouldBe Right(None)
  }

  test("an existing table is written by name when the path has a trailing slash and the location does not") {
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(GoldPath), GoldPath + "/") shouldBe Right(None)
  }

  test("an existing table is written by name when the location has repeated slashes") {
    val location =
      "abfss://application@stgoldhypatiapreneu.dfs.core.windows.net//usa_prodseries_migration_project//material_master"
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(location), GoldPath) shouldBe Right(None)
  }

  test("an existing table is written by name when scheme and authority differ only in case") {
    val location =
      "ABFSS://Application@StGoldHypatiaPreNeu.DFS.core.windows.net/usa_prodseries_migration_project/material_master"
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(location), GoldPath) shouldBe Right(None)
  }

  test("an existing table whose location differs in the case of the path part is rejected") {
    val location =
      "abfss://application@stgoldhypatiapreneu.dfs.core.windows.net/USA_prodseries_migration_project/Material_Master"
    val result = WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(location), GoldPath)
    result.isLeft shouldBe true
    leftMessage(result) should include(location)
  }

  test("an existing table in a different location is rejected naming the table, the location and the path") {
    val location = "abfss://application@stgoldhypatiaproneu.dfs.core.windows.net/usa_prodseries_migration_project/material_master"
    val result = WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(location), GoldPath)

    result.left.get shouldBe an[ADLS2ManagerException]
    val message = leftMessage(result)
    message should include(TableName)
    message should include(s"Existing location = $location")
    message should include(s"requested path = $GoldPath")
    message should include("does not match")
  }

  test("an existing table in another container is rejected") {
    val location = "abfss://raw@stgoldhypatiapreneu.dfs.core.windows.net/usa_prodseries_migration_project/material_master"
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(location), GoldPath).isLeft shouldBe true
  }

  test("an existing table whose location is a parent folder of the path is rejected") {
    val location = "abfss://application@stgoldhypatiapreneu.dfs.core.windows.net/usa_prodseries_migration_project"
    WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, Some(location), GoldPath).isLeft shouldBe true
  }

  test("an existing table with unknown location is rejected naming the table and the path") {
    val result = WriteTargetResolver.resolveWriteTarget(TableName, tableExists = true, None, GoldPath)

    result.left.get shouldBe an[ADLS2ManagerException]
    val message = leftMessage(result)
    message should include(TableName)
    message should include("location is unknown")
    message should include(s"Requested path = $GoldPath")
  }

  // ---------------------------------------------------------------------------------------------
  // samePath
  // ---------------------------------------------------------------------------------------------

  test("samePath is symmetric for equivalent paths") {
    val upper = "ABFSS://Application@StGoldHypatiaPreNeu.dfs.core.windows.net/usa_prodseries_migration_project/material_master/"
    WriteTargetResolver.samePath(upper, GoldPath) shouldBe true
    WriteTargetResolver.samePath(GoldPath, upper) shouldBe true
  }

  test("samePath is false for different tables in the same schema folder") {
    val other = "abfss://application@stgoldhypatiapreneu.dfs.core.windows.net/usa_prodseries_migration_project/plant_master"
    WriteTargetResolver.samePath(GoldPath, other) shouldBe false
  }

  test("samePath is false when only one table name is a prefix of the other") {
    WriteTargetResolver.samePath(GoldPath, GoldPath + "_v2") shouldBe false
  }

  test("samePath ignores surrounding blanks") {
    WriteTargetResolver.samePath(s"  $GoldPath\t", GoldPath) shouldBe true
  }

  // ---------------------------------------------------------------------------------------------
  // normalizePath
  // ---------------------------------------------------------------------------------------------

  test("normalizePath treats null as empty") {
    WriteTargetResolver.normalizePath(null) shouldBe ""
  }

  test("normalizePath turns a blank string into empty") {
    WriteTargetResolver.normalizePath("   ") shouldBe ""
    WriteTargetResolver.normalizePath("") shouldBe ""
  }

  test("normalizePath keeps an already normalized real path unchanged") {
    WriteTargetResolver.normalizePath(GoldPath) shouldBe GoldPath
  }

  test("normalizePath lowercases scheme and authority but keeps the case of the path") {
    WriteTargetResolver.normalizePath("ABFSS://Container@Account.dfs.core.windows.net/Folder/Table") shouldBe
      "abfss://container@account.dfs.core.windows.net/Folder/Table"
  }

  test("normalizePath collapses extra slashes right after the scheme") {
    WriteTargetResolver.normalizePath("abfss:///container@account.dfs.core.windows.net/folder") shouldBe
      "abfss://container@account.dfs.core.windows.net/folder"
  }

  test("normalizePath collapses repeated slashes inside the path and removes trailing slashes") {
    WriteTargetResolver.normalizePath("abfss://container@account.dfs.core.windows.net//a///b//") shouldBe
      "abfss://container@account.dfs.core.windows.net/a/b"
  }

  test("normalizePath handles a path with only scheme and authority") {
    WriteTargetResolver.normalizePath("abfss://Container@Account.dfs.core.windows.net/") shouldBe
      "abfss://container@account.dfs.core.windows.net"
    WriteTargetResolver.normalizePath("abfss://Container@Account.dfs.core.windows.net") shouldBe
      "abfss://container@account.dfs.core.windows.net"
  }

  test("normalizePath without scheme only collapses and trims slashes, keeping the case") {
    WriteTargetResolver.normalizePath(" /mnt//Gold/table/ ") shouldBe "/mnt/Gold/table"
  }
}
