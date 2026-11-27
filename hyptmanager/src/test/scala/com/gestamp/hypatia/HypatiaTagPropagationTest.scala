package com.gestamp.hypatia

import com.gestamp.hypatia.utils.tag_propagation._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class HypatiaTagPropagationTest extends FunSuite with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _
  var service: TagPropagationService = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("TagPropagationServiceTest")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    setupTestData()

    val mockAccessor = new MockSystemTableAccessor(spark)
    service = new TagPropagationService(spark, mockAccessor)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  def setupTestData(): Unit = {
    val sparkSession = spark
    import sparkSession.implicits._

    val currentDate = java.sql.Date.valueOf(java.time.LocalDate.now())

    // Test data for column tags
    Seq(
      ("silver", "sales", "transactions", "customer_id",  "PII",                "true"),
      ("silver", "sales", "transactions", "customer_id",  "sensitivity",        "high"),
      ("silver", "sales", "transactions", "company_code", "management_company", "GESTAMP"),
      ("gold",   "analytics", "customer_metrics", "customer_id", "PII", "true"),
      // Join por el mismo campo: ambas fuentes con el mismo governed tag
      ("silver", "logistics", "workplaces", "logisticCenter_code", "logistic_center", "logistic_center"),
      ("silver", "logistics", "shifts",     "logisticCenter_code", "logistic_center", "logistic_center"),
      // Campo combinado: una fuente con governed tag y otra sin él
      ("silver", "mixed", "with_tag", "lc_col", "logistic_center", "logistic_center"),
      // Tabla con una columna limpia (logistic_center) y otra combinada (management_company + fuente sin tag)
      ("silver", "combo", "clean",  "lc_col", "logistic_center",   "logistic_center"),
      ("silver", "combo", "mc_src", "mc_col", "management_company", "management_company"),
      // Mismo campo en dos tablas, solo una con governed tag (la otra sin etiquetar)
      ("silver", "samefield", "workreferences", "logisticCenter_code", "logistic_center", "logistic_center")
    ).toDF("catalog_name", "schema_name", "table_name", "column_name", "tag_name", "tag_value")
      .createOrReplaceTempView("mock_column_tags")

    // Test data for columns
    // company_info es un campo calculado (multi-fuente) que concatena customer_id y company_code
    Seq(
      ("gold", "analytics", "customer_metrics", "customer_id",   1),
      ("gold", "analytics", "customer_metrics", "total_amount",  2),
      ("gold", "analytics", "customer_metrics", "company_info",  3),
      ("gold", "analytics", "kpi_workplace", "workplace_id",        1),
      ("gold", "analytics", "kpi_workplace", "logisticCenter_code", 2),
      ("gold", "analytics", "mixed_table",   "combined_col",        1),
      ("gold", "analytics", "combo_table",   "combined_col",        1),
      ("gold", "analytics", "combo_table",   "clean_col",           2),
      ("gold", "analytics", "samefield_table", "calendarYearMonth",    1),
      ("gold", "analytics", "samefield_table", "logisticCenter_code",  2)
    ).toDF("table_catalog", "table_schema", "table_name", "column_name", "ordinal_position")
      .createOrReplaceTempView("mock_columns")

    // Test data for column lineage
    // company_info tiene dos fuentes Silver -> campo calculado/concatenado
    Seq(
      ("gold.analytics.customer_metrics", "customer_id",  "silver.sales.transactions", "silver", "sales", "transactions", "customer_id",  currentDate),
      ("gold.analytics.customer_metrics", "company_info", "silver.sales.transactions", "silver", "sales", "transactions", "customer_id",  currentDate),
      ("gold.analytics.customer_metrics", "company_info", "silver.sales.transactions", "silver", "sales", "transactions", "company_code", currentDate),
      ("gold.analytics.kpi_workplace", "logisticCenter_code", "silver.logistics.workplaces", "silver", "logistics", "workplaces", "logisticCenter_code", currentDate),
      ("gold.analytics.kpi_workplace", "logisticCenter_code", "silver.logistics.shifts",     "silver", "logistics", "shifts",     "logisticCenter_code", currentDate),
      ("gold.analytics.mixed_table", "combined_col", "silver.mixed.with_tag",    "silver", "mixed", "with_tag",    "lc_col",    currentDate),
      ("gold.analytics.mixed_table", "combined_col", "silver.mixed.without_tag", "silver", "mixed", "without_tag", "plain_col", currentDate),
      ("gold.analytics.combo_table", "clean_col",    "silver.combo.clean",        "silver", "combo", "clean",        "lc_col",    currentDate),
      ("gold.analytics.combo_table", "combined_col", "silver.combo.mc_src",       "silver", "combo", "mc_src",       "mc_col",    currentDate),
      ("gold.analytics.combo_table", "combined_col", "silver.combo.mc_src_plain", "silver", "combo", "mc_src_plain", "plain_col", currentDate),
      ("gold.analytics.samefield_table", "logisticCenter_code", "silver.samefield.workreferences", "silver", "samefield", "workreferences", "logisticCenter_code", currentDate),
      ("gold.analytics.samefield_table", "logisticCenter_code", "silver.samefield.workplaces",     "silver", "samefield", "workplaces",     "logisticCenter_code", currentDate)
    ).toDF("target_table_full_name", "target_column_name", "source_table_full_name",
      "source_table_catalog", "source_table_schema", "source_table_name", "source_column_name", "event_date")
      .createOrReplaceTempView("mock_column_lineage")
  }

  test("getColumnTags returns tags for a column") {
    val tags = service.getColumnTags("silver", "sales", "transactions", "customer_id")
    tags.size shouldBe 2
    tags.map(_.key) should contain allOf("PII", "sensitivity")
  }

  test("getAllColumnTags returns all column tags for a table") {
    val allTags = service.getAllColumnTags("silver", "sales", "transactions")
    allTags.keySet should contain("customer_id")
    allTags("customer_id").size shouldBe 2
  }

  test("getTableColumns returns column names") {
    val columns = service.getTableColumns("gold", "analytics", "customer_metrics")
    columns.size shouldBe 3
    columns should contain allOf("customer_id", "total_amount", "company_info")
  }

  test("getColumnLineageToLayer returns silver sources") {
    val sources = service.getColumnLineageToLayer("gold", "analytics", "customer_metrics", "customer_id", "silver", 30)
    sources should not be empty
    sources.head.catalog shouldBe "silver"
    sources.head.tags should not be empty
  }

  test("getTagPropagationPlan identifies tags to propagate") {
    val plan = service.getTagPropagationPlan("gold", "analytics", "customer_metrics", "silver", 30)
    plan should not be empty
    plan.exists(_.targetColumn.columnName == "customer_id") shouldBe true
  }

  test("catalog/schema/table names are matched case-insensitively (Unity Catalog lowercases them)") {
    // Reproduce el caso del log: una tabla registrada con mayúsculas (p. ej. HeadcountActual) que UC guarda en
    // minúsculas. Antes de normalizar, estas consultas devolvían 0 columnas y no se propagaba nada.
    val plan = service.getTagPropagationPlan("GOLD", "Analytics", "Customer_Metrics", "SILVER", 30)
    plan should not be empty
    plan.exists(_.targetColumn.columnName == "customer_id") shouldBe true
    // el nombre completo de la tabla sale normalizado
    plan.find(_.targetColumn.columnName == "customer_id").get.targetColumn.fullTableName shouldBe
      "gold.analytics.customer_metrics"
  }

  test("generateTagPropagationSQL creates ALTER TABLE statements") {
    val plan = Seq(TagPropagationPlan(
      ColumnInfo("gold", "analytics", "customer_metrics", "customer_id"),
      Seq(Tag("sensitivity", "high")),
      Seq.empty
    ))
    val sql = service.generateTagPropagationSQL(plan)
    sql should not be empty
    sql.head should include("ALTER TABLE")
    sql.head should include("SET TAGS")
  }

  test("generatePropagationReport creates report DataFrame") {
    val plan = Seq(TagPropagationPlan(
      ColumnInfo("gold", "analytics", "customer_metrics", "customer_id", Seq(Tag("PII", "true"))),
      Seq(Tag("sensitivity", "high")),
      Seq(ColumnInfo("silver", "sales", "transactions", "customer_id", Seq(Tag("sensitivity", "high"))))
    ))
    val report = service.generatePropagationReport(plan)
    report.count() shouldBe 1
    val row = report.collect().head
    row.getAs[String]("gold_column") shouldBe "customer_id"
    row.getAs[Boolean]("needs_update") shouldBe true
  }

  test("executeTagPropagation performs dry run") {
    val plan = Seq(TagPropagationPlan(
      ColumnInfo("gold", "analytics", "customer_metrics", "customer_id"),
      Seq(Tag("test", "value")),
      Seq.empty
    ))
    val results = service.executeTagPropagation(plan, dryRun = true)
    results.size shouldBe 1
    results.head._2 shouldBe true
    results.head._3 shouldBe "Dry run - not executed"
  }

  test("resolveGovernedTags: columna multi-fuente no hereda governed tags") {
    val plan = service.getTagPropagationPlan("gold", "analytics", "customer_metrics", "silver", 30)
    val companyInfoPlan = plan.find(_.targetColumn.columnName == "company_info")
    companyInfoPlan shouldBe defined
    companyInfoPlan.get.tagsToPropagate.map(_.key) should not contain "management_company"
  }

  test("resolveGovernedTags: aplica blocked cuando governed tag se pierde por campo calculado") {
    val plan = service.getTagPropagationPlan("gold", "analytics", "customer_metrics", "silver", 30)
    val firstColPlan = plan.find(_.targetColumn.columnName == "customer_id")
    firstColPlan shouldBe defined
    val blockedTag = firstColPlan.get.tagsToPropagate.find(_.key == "blocked")
    blockedTag shouldBe defined
    blockedTag.get.value shouldBe "management_company"
  }

  test("resolveGovernedTags: columna multi-fuente con el mismo governed tag en todas las fuentes lo conserva") {
    val plan = service.getTagPropagationPlan("gold", "analytics", "kpi_workplace", "silver", 30)
    val tags = plan.flatMap(_.tagsToPropagate)
    plan.find(_.targetColumn.columnName == "logisticCenter_code").get.tagsToPropagate should contain(Tag("logistic_center", "logistic_center"))
    tags.map(_.key) should not contain "blocked"
  }

  test("resolveGovernedTags: columna multi-fuente con una fuente sin governed tag no lo hereda y aplica blocked") {
    val plan = service.getTagPropagationPlan("gold", "analytics", "mixed_table", "silver", 30)
    val tags = plan.flatMap(_.tagsToPropagate)
    tags.map(_.key) should not contain "logistic_center"
    tags should contain(Tag("blocked", "logistic_center"))
  }

  test("resolveGovernedTags: columna limpia gana sobre columna combinada, aunque la combinada traiga mayor prioridad") {
    val plan = service.getTagPropagationPlan("gold", "analytics", "combo_table", "silver", 30)
    val tags = plan.flatMap(p => p.tagsToPropagate.map(t => p.targetColumn.columnName -> t))
    tags should contain("clean_col" -> Tag("logistic_center", "logistic_center"))
    tags.map(_._2.key) should not contain "management_company"
    tags.map(_._2.key) should not contain "blocked"
  }

  test("resolveGovernedTags: mismo campo en dos tablas, solo una etiquetada, conserva el tag y no aplica blocked") {
    val plan = service.getTagPropagationPlan("gold", "analytics", "samefield_table", "silver", 30)
    val tags = plan.flatMap(_.tagsToPropagate)
    plan.find(_.targetColumn.columnName == "logisticCenter_code").get.tagsToPropagate should contain(Tag("logistic_center", "logistic_center"))
    tags.map(_.key) should not contain "blocked"
  }

  test("ColumnInfo generates correct full names") {
    val col = ColumnInfo("catalog", "schema", "table", "column")
    col.fullTableName shouldBe "catalog.schema.table"
    col.fullColumnName shouldBe "catalog.schema.table.column"
  }

}
