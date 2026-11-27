package com.gestamp.hypatia

import com.gestamp.hypatia.utils.tag_propagation._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
 * Business scenarios of [[TagPropagationService.syncTagsToDesiredState]] (Overwrite without DROP) over the
 * [[MockSystemTableAccessor]] temp views. Same session pattern as HypatiaTagPropagationTest (getOrCreate):
 * the module has no BaseTest / SparkSessionMock. The temp views are recreated in beforeAll, so this suite
 * must not run in parallel with HypatiaTagPropagationTest (scalatest-maven-plugin runs suites sequentially).
 */
class TagSyncToDesiredStateTest extends FunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var service: TagPropagationService = _

  private val Silver = "silver"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("TagSyncToDesiredStateTest")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    setupTestData()
    service = new TagPropagationService(spark, new MockSystemTableAccessor(spark))
  }

  private def setupTestData(): Unit = {
    val sparkSession = spark
    import sparkSession.implicits._
    val today = java.sql.Date.valueOf(java.time.LocalDate.now())

    Seq(
      // (1) Silver: plant_code keeps logistic_center, company_code now brings management_company
      ("silver", "sync", "plants",    "plant_code",   "logistic_center",    "LC1"),
      ("silver", "sync", "companies", "company_code", "management_company", "MC1"),
      ("silver", "sync", "companies", "company_code", "PII",                "false"),
      // (1) Gold today: the old logistic_center (from before Silver brought management_company) and PII
      ("gold", "sync", "mc_switch", "plant_code",   "logistic_center", "LC1"),
      ("gold", "sync", "mc_switch", "company_code", "PII",             "false"),
      // (2) Excluded table: Gold has tags, Silver too
      ("gold", "sync", "excluded_tbl", "a", "PII",                "true"),
      ("gold", "sync", "excluded_tbl", "b", "management_company", "MC1"),
      ("silver", "sync", "excl_src", "a", "PII",                "true"),
      ("silver", "sync", "excl_src", "b", "management_company", "MC1"),
      // (3) No lineage: Gold has a governed tag
      ("gold", "sync", "no_lineage", "a", "logistic_center", "LC1"),
      ("gold", "sync", "bronze_only", "a", "logistic_center", "LC1"),
      // (4) Lineage to an untagged Silver source: Gold keeps an obsolete tag
      ("gold", "sync", "untagged_src", "a", "PII", "true"),
      // (5) Blocked: calc_col combines a governed Silver field with an untagged one
      ("silver", "sync", "lc_src", "lc_col", "logistic_center", "LC1"),
      ("gold", "sync", "blocked_tbl", "calc_col", "logistic_center", "LC1"),
      // In sync table: Gold already has exactly the Silver tags
      ("silver", "sync", "insync_src", "a", "PII", "true"),
      ("gold",   "sync", "in_sync",    "a", "PII", "true"),
      // Partial lineage: the governed column has no Silver lineage, another column has (untagged) sources
      ("gold", "sync", "partial_lineage", "lc_col",     "logistic_center", "LC1"),
      ("gold", "sync", "partial_lineage", "other_col",  "PII",             "true"),
      ("gold", "sync", "partial_lineage", "bronze_col", "PII",             "true"),
      // Blocked to governed: first_col (no lineage) has blocked, lc_col now gets logistic_center from Silver
      ("gold", "sync", "blocked_to_governed", "first_col", "blocked", "logistic_center")
    ).toDF("catalog_name", "schema_name", "table_name", "column_name", "tag_name", "tag_value")
      .createOrReplaceTempView("mock_column_tags")

    Seq(
      ("gold", "sync", "mc_switch",    "plant_code",   1),
      ("gold", "sync", "mc_switch",    "company_code", 2),
      ("gold", "sync", "excluded_tbl", "a", 1),
      ("gold", "sync", "excluded_tbl", "b", 2),
      ("gold", "sync", "no_lineage",   "a", 1),
      ("gold", "sync", "bronze_only",  "a", 1),
      ("gold", "sync", "untagged_src", "a", 1),
      ("gold", "sync", "blocked_tbl",  "first_col", 1),
      ("gold", "sync", "blocked_tbl",  "calc_col",  2),
      ("gold", "sync", "in_sync",      "a", 1),
      ("gold", "sync", "partial_lineage", "lc_col",     1),
      ("gold", "sync", "partial_lineage", "other_col",  2),
      ("gold", "sync", "partial_lineage", "bronze_col", 3),
      ("gold", "sync", "blocked_to_governed", "first_col", 1),
      ("gold", "sync", "blocked_to_governed", "lc_col",    2)
    ).toDF("table_catalog", "table_schema", "table_name", "column_name", "ordinal_position")
      .createOrReplaceTempView("mock_columns")

    Seq(
      ("gold.sync.mc_switch", "plant_code",   "silver.sync.plants",    "silver", "sync", "plants",    "plant_code",   today),
      ("gold.sync.mc_switch", "company_code", "silver.sync.companies", "silver", "sync", "companies", "company_code", today),
      ("gold.sync.excluded_tbl", "a", "silver.sync.excl_src", "silver", "sync", "excl_src", "a", today),
      ("gold.sync.excluded_tbl", "b", "silver.sync.excl_src", "silver", "sync", "excl_src", "b", today),
      ("gold.sync.bronze_only", "a", "bronze.sync.raw_src", "bronze", "sync", "raw_src", "a", today),
      ("gold.sync.untagged_src", "a", "silver.sync.plain", "silver", "sync", "plain", "a", today),
      ("gold.sync.blocked_tbl", "calc_col", "silver.sync.lc_src",    "silver", "sync", "lc_src",    "lc_col",    today),
      ("gold.sync.blocked_tbl", "calc_col", "silver.sync.plain_src", "silver", "sync", "plain_src", "other_col", today),
      ("gold.sync.in_sync", "a", "silver.sync.insync_src", "silver", "sync", "insync_src", "a", today),
      ("gold.sync.partial_lineage", "other_col",  "silver.sync.plain",   "silver", "sync", "plain",   "a", today),
      ("gold.sync.partial_lineage", "bronze_col", "bronze.sync.raw_src", "bronze", "sync", "raw_src", "a", today),
      ("gold.sync.blocked_to_governed", "lc_col", "silver.sync.lc_src", "silver", "sync", "lc_src", "lc_col", today)
    ).toDF("target_table_full_name", "target_column_name", "source_table_full_name",
      "source_table_catalog", "source_table_schema", "source_table_name", "source_column_name", "event_date")
      .createOrReplaceTempView("mock_column_lineage")
  }

  private def sync(table: String, excluded: Boolean = false, dryRun: Boolean = true,
                   svc: TagPropagationService = service): Seq[(String, Boolean, String)] =
    svc.syncTagsToDesiredState("gold", "sync", table, Silver, daysBack = 30, excluded = excluded, dryRun = dryRun)

  private def sqls(results: Seq[(String, Boolean, String)]): Seq[String] = results.map(_._1)

  private def set(table: String, col: String, key: String, value: String): String =
    s"ALTER TABLE gold.sync.$table ALTER COLUMN $col SET TAGS ('$key' = '$value');"

  private def unset(table: String, col: String, key: String): String =
    s"ALTER TABLE gold.sync.$table ALTER COLUMN $col UNSET TAGS ('$key');"

  // (1) ------------------------------------------------------------------------------------------

  test("Silver now brings management_company: SET management_company and UNSET the old logistic_center") {
    val results = sync("mc_switch")

    sqls(results) shouldBe Seq(
      set("mc_switch", "company_code", "management_company", "MC1"),
      unset("mc_switch", "plant_code", "logistic_center"))
    results.foreach { case (_, ok, msg) => ok shouldBe true; msg shouldBe "Dry run - not executed" }
  }

  test("Silver now brings management_company: every SET comes before every UNSET") {
    val statements = sqls(sync("mc_switch"))
    val lastSet = statements.lastIndexWhere(_.contains(" SET TAGS "))
    val firstUnset = statements.indexWhere(_.contains(" UNSET TAGS "))

    lastSet should be >= 0
    firstUnset should be > lastSet
  }

  test("the desired plan applies one governed tag per table and ignores the tags Gold already has") {
    val desired = service.getDesiredTagPropagationPlan("gold", "sync", "mc_switch", Silver, 30)
    val tags = desired.flatMap(p => p.tagsToPropagate.map(t => p.targetColumn.columnName -> t))

    tags should contain("company_code" -> Tag("management_company", "MC1"))
    // PII is already in Gold: the additive plan drops it, the desired plan keeps it
    tags should contain("company_code" -> Tag("PII", "false"))
    tags.map(_._2.key) should not contain "logistic_center"
    tags.map(_._2.key) should not contain "blocked"

    val additive = service.getTagPropagationPlan("gold", "sync", "mc_switch", Silver, 30)
    additive.flatMap(_.tagsToPropagate) should not contain Tag("PII", "false")
  }

  test("if a SET fails no UNSET is executed") {
    // dryRun = false on local Spark: ALTER ... SET TAGS is not valid OSS Spark SQL, so every SET fails
    val results = sync("mc_switch", dryRun = false)

    sqls(results) shouldBe Seq(set("mc_switch", "company_code", "management_company", "MC1"))
    results.head._2 shouldBe false
    sqls(results).exists(_.contains("UNSET")) shouldBe false
  }

  // (2) ------------------------------------------------------------------------------------------

  test("an excluded table only gets UNSET statements for all its current tags") {
    val results = sync("excluded_tbl", excluded = true)

    sqls(results) shouldBe Seq(
      unset("excluded_tbl", "a", "PII"),
      unset("excluded_tbl", "b", "management_company"))
    results.forall(_._2) shouldBe true
  }

  test("an excluded table without tags produces no statement") {
    sync("missing_table", excluded = true) shouldBe empty
  }

  // (3) ------------------------------------------------------------------------------------------

  test("with no lineage at all the current Gold tags are kept (no UNSET)") {
    sync("no_lineage") shouldBe empty
  }

  test("with lineage only to non Silver sources the current Gold tags are kept (no UNSET)") {
    sync("bronze_only") shouldBe empty
  }

  // (4) ------------------------------------------------------------------------------------------

  test("a Silver source found but without tags clears the obsolete Gold tags") {
    sqls(sync("untagged_src")) shouldBe Seq(unset("untagged_src", "a", "PII"))
  }

  // (5) ------------------------------------------------------------------------------------------

  test("blocked: Silver had a governed tag that Gold does not keep, even if Gold had it before the overwrite") {
    sqls(sync("blocked_tbl")) shouldBe Seq(
      set("blocked_tbl", "first_col", "blocked", "logistic_center"),
      unset("blocked_tbl", "calc_col", "logistic_center"))
  }

  test("blocked is part of the desired plan on the first Gold column") {
    val desired = service.getDesiredTagPropagationPlan("gold", "sync", "blocked_tbl", Silver, 30)

    desired.find(_.targetColumn.columnName == "first_col").map(_.tagsToPropagate) shouldBe
      Some(Seq(Tag("blocked", "logistic_center")))
    desired.flatMap(_.tagsToPropagate).map(_.key) should not contain "logistic_center"
  }

  // Others ---------------------------------------------------------------------------------------

  test("a table already in the desired state produces no statement") {
    sync("in_sync") shouldBe empty
  }

  test("if the lineage cannot be read the sync fails without executing anything") {
    val failingAccessor = new MockSystemTableAccessor(spark) {
      override def getColumnLineage(targetTable: TableRef, columnName: String, daysBack: Int): DataFrame =
        throw new RuntimeException("column_lineage unavailable")
    }
    val failingService = new TagPropagationService(spark, failingAccessor)

    val e = intercept[RuntimeException](sync("mc_switch", svc = failingService))
    e.getMessage shouldBe "column_lineage unavailable"
  }

  test("an excluded table does not read the lineage, so a lineage failure does not prevent the UNSET") {
    val failingAccessor = new MockSystemTableAccessor(spark) {
      override def getColumnLineage(targetTable: TableRef, columnName: String, daysBack: Int): DataFrame =
        throw new RuntimeException("column_lineage unavailable")
    }
    val failingService = new TagPropagationService(spark, failingAccessor)

    sqls(sync("excluded_tbl", excluded = true, svc = failingService)) should have size 2
  }

  // Per-column lineage guard ------------------------------------------------------------------

  test("a governed tag on a column without Silver lineage is kept while a column with lineage is cleaned") {
    val statements = sqls(sync("partial_lineage"))

    // lc_col (no lineage) keeps logistic_center: the table does not lose its row filter
    statements shouldBe Seq(unset("partial_lineage", "other_col", "PII"))
    statements.exists(_.contains("lc_col")) shouldBe false
  }

  test("a column whose lineage only has non Silver sources counts as without lineage and keeps its tags") {
    sqls(sync("partial_lineage")).exists(_.contains("bronze_col")) shouldBe false
  }

  test("an excluded table with partial lineage still unsets every tag") {
    sqls(sync("partial_lineage", excluded = true)) shouldBe Seq(
      unset("partial_lineage", "bronze_col", "PII"),
      unset("partial_lineage", "lc_col", "logistic_center"),
      unset("partial_lineage", "other_col", "PII"))
  }

  // Blocked to governed ---------------------------------------------------------------------------

  test("blocked on a column without lineage is unset once the governed tag reaches another column, SET first") {
    sqls(sync("blocked_to_governed")) shouldBe Seq(
      set("blocked_to_governed", "lc_col", "logistic_center", "LC1"),
      unset("blocked_to_governed", "first_col", "blocked"))
  }
}
