package com.gestamp.hypatia.utils.tag_propagation

import org.scalatest.{FunSuite, Matchers}

/**
 * Pure tests (no Spark) for [[TagSyncDiff]]: the SET / UNSET diff between the desired and the current column tags
 * of a Gold table, the empty-lineage guard and the SQL statements.
 */
class TagSyncDiffTest extends FunSuite with Matchers {

  private val Table = "gold.analytics.kpi"

  private def plan(column: String, tags: Tag*): TagPropagationPlan =
    TagPropagationPlan(ColumnInfo("gold", "analytics", "kpi", column), tags, Seq.empty)

  private def change(column: String, key: String, value: String): ColumnTagChange =
    ColumnTagChange(column, Tag(key, value))

  // ---------------------------------------------------------------------------------------------
  // compute
  // ---------------------------------------------------------------------------------------------

  test("compute returns an empty diff when the current tags already match the desired ones") {
    val desired = Seq(plan("plant_code", Tag("logistic_center", "LC1")), plan("amount", Tag("PII", "false")))
    val current = Map("plant_code" -> Seq(Tag("logistic_center", "LC1")), "amount" -> Seq(Tag("PII", "false")))

    TagSyncDiff.compute(desired, current, Seq("plant_code", "amount")) shouldBe TagSyncDiff(Seq.empty, Seq.empty)
  }

  test("compute returns an empty diff when there are neither desired nor current tags") {
    TagSyncDiff.compute(Seq.empty, Map.empty, Seq("a", "b")) shouldBe TagSyncDiff(Seq.empty, Seq.empty)
  }

  test("compute sets a desired tag that the column does not have") {
    val diff = TagSyncDiff.compute(Seq(plan("company_code", Tag("management_company", "MC1"))), Map.empty,
      Seq("company_code"))

    diff.toSet shouldBe Seq(change("company_code", "management_company", "MC1"))
    diff.toUnset shouldBe empty
  }

  test("compute sets a tag whose value changed and does not unset the same key") {
    val diff = TagSyncDiff.compute(
      Seq(plan("company_code", Tag("management_company", "NEW"))),
      Map("company_code" -> Seq(Tag("management_company", "OLD"))),
      Seq("company_code"))

    diff.toSet shouldBe Seq(change("company_code", "management_company", "NEW"))
    diff.toUnset shouldBe empty
  }

  test("compute unsets a current tag that is not in the desired state, with its current value") {
    val diff = TagSyncDiff.compute(
      Seq(plan("plant_code", Tag("PII", "true"))),
      Map("plant_code" -> Seq(Tag("PII", "true"), Tag("logistic_center", "LC1"))),
      Seq("plant_code"))

    diff.toSet shouldBe empty
    diff.toUnset shouldBe Seq(change("plant_code", "logistic_center", "LC1"))
  }

  test("compute with an empty desired state unsets every current tag, ordered by column") {
    val current = Map("zeta" -> Seq(Tag("PII", "true")), "alpha" -> Seq(Tag("management_company", "MC1")))

    val diff = TagSyncDiff.compute(Seq.empty, current, Seq("zeta", "alpha"))

    diff.toSet shouldBe empty
    diff.toUnset shouldBe Seq(change("alpha", "management_company", "MC1"), change("zeta", "PII", "true"))
  }

  test("compute ignores desired and current tags of columns that no longer exist in the table") {
    val diff = TagSyncDiff.compute(
      Seq(plan("dropped_col", Tag("PII", "true")), plan("kept_col", Tag("PII", "true"))),
      Map("dropped_col" -> Seq(Tag("management_company", "MC1")), "kept_col" -> Seq.empty),
      Seq("kept_col"))

    diff.toSet shouldBe Seq(change("kept_col", "PII", "true"))
    diff.toUnset shouldBe empty
  }

  test("compute matches column names ignoring case and uses the real column name of the table") {
    val diff = TagSyncDiff.compute(
      Seq(plan("LOGISTICCENTER_CODE", Tag("logistic_center", "LC1"))),
      Map("logisticcenter_code" -> Seq(Tag("PII", "true"))),
      Seq("logisticCenter_code"))

    diff.toSet shouldBe Seq(change("logisticCenter_code", "logistic_center", "LC1"))
    diff.toUnset shouldBe Seq(change("logisticCenter_code", "PII", "true"))
    TagSyncDiff.setTagSQL(Table, diff.toSet.head) should include("ALTER COLUMN logisticCenter_code SET TAGS")
    TagSyncDiff.unsetTagSQL(Table, diff.toUnset.head) should include("ALTER COLUMN logisticCenter_code UNSET TAGS")
  }

  test("compute does not set a tag that already exists on a column named with a different case") {
    val diff = TagSyncDiff.compute(
      Seq(plan("Company_Code", Tag("management_company", "MC1"))),
      Map("COMPANY_CODE" -> Seq(Tag("management_company", "MC1"))),
      Seq("company_code"))

    diff shouldBe TagSyncDiff(Seq.empty, Seq.empty)
  }

  test("compute keeps the last value when the plan has several values for the same column and key") {
    val desired = Seq(plan("company_code", Tag("management_company", "MC1")),
      plan("company_code", Tag("management_company", "MC2")))

    val diff = TagSyncDiff.compute(desired, Map.empty, Seq("company_code"))

    diff.toSet shouldBe Seq(change("company_code", "management_company", "MC2"))
  }

  test("compute sets nothing when the current value equals the last desired value of a repeated key") {
    val desired = Seq(plan("company_code", Tag("management_company", "MC1"), Tag("management_company", "MC2")))

    val diff = TagSyncDiff.compute(desired, Map("company_code" -> Seq(Tag("management_company", "MC2"))),
      Seq("company_code"))

    diff shouldBe TagSyncDiff(Seq.empty, Seq.empty)
  }

  test("compute keeps the SET statements in the order of the plan") {
    val desired = Seq(plan("b", Tag("k2", "v")), plan("a", Tag("k1", "v")), plan("b", Tag("k3", "v")))

    TagSyncDiff.compute(desired, Map.empty, Seq("a", "b")).toSet shouldBe
      Seq(change("b", "k2", "v"), change("a", "k1", "v"), change("b", "k3", "v"))
  }

  test("compute compares tag keys with their case") {
    val diff = TagSyncDiff.compute(Seq(plan("a", Tag("PII", "true"))), Map("a" -> Seq(Tag("pii", "true"))), Seq("a"))

    diff.toSet shouldBe Seq(change("a", "PII", "true"))
    diff.toUnset shouldBe Seq(change("a", "pii", "true"))
  }

  test("compute with unsetAllowed = false leaves toUnset empty and keeps toSet") {
    val desired = Seq(plan("company_code", Tag("management_company", "MC1")))
    val current = Map("plant_code" -> Seq(Tag("logistic_center", "LC1")))

    val allowed = TagSyncDiff.compute(desired, current, Seq("company_code", "plant_code"))
    val notAllowed = TagSyncDiff.compute(desired, current, Seq("company_code", "plant_code"), unsetAllowed = false)

    allowed.toUnset should not be empty
    notAllowed.toUnset shouldBe empty
    notAllowed.toSet shouldBe allowed.toSet
    notAllowed.toSet shouldBe Seq(change("company_code", "management_company", "MC1"))
  }

  test("compute with an empty table column list produces an empty diff") {
    TagSyncDiff.compute(Seq(plan("a", Tag("PII", "true"))), Map("a" -> Seq(Tag("x", "y"))), Seq.empty) shouldBe
      TagSyncDiff(Seq.empty, Seq.empty)
  }

  // ---------------------------------------------------------------------------------------------
  // unsetAllowed
  // ---------------------------------------------------------------------------------------------

  test("unsetAllowed is false for a non excluded table with no Silver source in the lineage") {
    TagSyncDiff.unsetAllowed(excluded = false, lineageSourceCount = 0) shouldBe false
  }

  test("unsetAllowed is true for a non excluded table with Silver sources in the lineage") {
    TagSyncDiff.unsetAllowed(excluded = false, lineageSourceCount = 1) shouldBe true
    TagSyncDiff.unsetAllowed(excluded = false, lineageSourceCount = 42) shouldBe true
  }

  test("unsetAllowed is always true for an excluded table") {
    TagSyncDiff.unsetAllowed(excluded = true, lineageSourceCount = 0) shouldBe true
    TagSyncDiff.unsetAllowed(excluded = true, lineageSourceCount = 3) shouldBe true
  }

  // ---------------------------------------------------------------------------------------------
  // SQL
  // ---------------------------------------------------------------------------------------------

  test("setTagSQL builds the exact SET TAGS statement") {
    TagSyncDiff.setTagSQL(Table, change("company_code", "management_company", "MC1")) shouldBe
      "ALTER TABLE gold.analytics.kpi ALTER COLUMN company_code SET TAGS ('management_company' = 'MC1');"
  }

  test("unsetTagSQL builds the exact UNSET TAGS statement, without the value") {
    TagSyncDiff.unsetTagSQL(Table, change("plant_code", "logistic_center", "LC1")) shouldBe
      "ALTER TABLE gold.analytics.kpi ALTER COLUMN plant_code UNSET TAGS ('logistic_center');"
  }

  test("setTagSQL uses the same format as the existing additive propagation") {
    val sql = new TagPropagationService(null).generateTagPropagationSQL(
      Seq(TagPropagationPlan(ColumnInfo("gold", "analytics", "kpi", "company_code"),
        Seq(Tag("management_company", "MC1")), Seq.empty)))

    sql shouldBe Seq(TagSyncDiff.setTagSQL(Table, change("company_code", "management_company", "MC1")))
  }

  // ---------------------------------------------------------------------------------------------
  // compute: per-column lineage guard (columnsWithLineage)
  // ---------------------------------------------------------------------------------------------

  private val currentTwoCols = Map(
    "lc_col" -> Seq(Tag("logistic_center", "LC1")),
    "other_col" -> Seq(Tag("PII", "true")))
  private val twoCols = Seq("lc_col", "other_col")

  test("compute keeps the tags of a column without lineage and reports them in keptByMissingLineage") {
    val diff = TagSyncDiff.compute(Seq.empty, Map("lc_col" -> Seq(Tag("logistic_center", "LC1"))), Seq("lc_col"),
      columnsWithLineage = Some(Set.empty))

    diff.toUnset shouldBe empty
    diff.keptByMissingLineage shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
  }

  test("compute cleans the obsolete tags of a column with lineage") {
    val diff = TagSyncDiff.compute(Seq.empty, Map("other_col" -> Seq(Tag("PII", "true"))), Seq("other_col"),
      columnsWithLineage = Some(Set("other_col")))

    diff.toUnset shouldBe Seq(change("other_col", "PII", "true"))
    diff.keptByMissingLineage shouldBe empty
  }

  test("compute with columns with and without lineage only unsets the ones with lineage") {
    val diff = TagSyncDiff.compute(Seq.empty, currentTwoCols, twoCols, columnsWithLineage = Some(Set("other_col")))

    diff.toUnset shouldBe Seq(change("other_col", "PII", "true"))
    diff.keptByMissingLineage shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
  }

  test("compute matches columnsWithLineage ignoring case and reports the real column name") {
    val diff = TagSyncDiff.compute(Seq.empty,
      Map("LogisticCenter_Code" -> Seq(Tag("logistic_center", "LC1")), "Other_Col" -> Seq(Tag("PII", "true"))),
      Seq("logisticCenter_code", "other_col"), columnsWithLineage = Some(Set("OTHER_COL")))

    diff.toUnset shouldBe Seq(change("other_col", "PII", "true"))
    diff.keptByMissingLineage shouldBe Seq(change("logisticCenter_code", "logistic_center", "LC1"))
  }

  test("compute with columnsWithLineage = None unsets every obsolete tag and keeps nothing") {
    val diff = TagSyncDiff.compute(Seq.empty, currentTwoCols, twoCols, columnsWithLineage = None)

    diff.toUnset shouldBe Seq(change("lc_col", "logistic_center", "LC1"), change("other_col", "PII", "true"))
    diff.keptByMissingLineage shouldBe empty
  }

  test("compute with unsetAllowed = false empties both toUnset and keptByMissingLineage") {
    val diff = TagSyncDiff.compute(Seq.empty, currentTwoCols, twoCols, unsetAllowed = false,
      columnsWithLineage = Some(Set("other_col")))

    diff.toUnset shouldBe empty
    diff.keptByMissingLineage shouldBe empty
  }

  test("the per-column lineage guard does not affect the SET statements") {
    val desired = Seq(plan("lc_col", Tag("blocked", "logistic_center")), plan("other_col", Tag("PII", "false")))

    val guarded = TagSyncDiff.compute(desired, currentTwoCols, twoCols, columnsWithLineage = Some(Set.empty))
    val unguarded = TagSyncDiff.compute(desired, currentTwoCols, twoCols)

    guarded.toSet shouldBe unguarded.toSet
    guarded.toSet shouldBe Seq(change("lc_col", "blocked", "logistic_center"), change("other_col", "PII", "false"))
    guarded.toUnset shouldBe empty
    guarded.keptByMissingLineage shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
  }

  // ---------------------------------------------------------------------------------------------
  // compute: governed / table level keys and the per-column lineage guard
  // ---------------------------------------------------------------------------------------------

  private val Governed = TagPropagationService.GovernedTagKeys
  private val TableLevel = Set(TagPropagationService.BlockedTagKey)

  private def guarded(desired: Seq[TagPropagationPlan], current: Map[String, Seq[Tag]], columns: Seq[String],
                      withLineage: Set[String], unsetAllowed: Boolean = true): TagSyncDiff =
    TagSyncDiff.compute(desired, current, columns, unsetAllowed, Some(withLineage), Governed, TableLevel)

  test("a blocked tag on a column without lineage is unset when the desired state has no blocked") {
    val current = Map("first_col" -> Seq(Tag("blocked", "logistic_center")))
    val desired = Seq(plan("lc_col", Tag("logistic_center", "LC1")))

    val diff = guarded(desired, current, Seq("first_col", "lc_col"), withLineage = Set("lc_col"))

    diff.toSet shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
    diff.toUnset shouldBe Seq(change("first_col", "blocked", "logistic_center"))
    diff.keptByMissingLineage shouldBe empty
  }

  test("a blocked tag is outside the column guard even when the desired state is empty") {
    val diff = guarded(Seq.empty, Map("first_col" -> Seq(Tag("blocked", "logistic_center"))), Seq("first_col"),
      withLineage = Set.empty)

    diff.toUnset shouldBe Seq(change("first_col", "blocked", "logistic_center"))
  }

  test("a blocked tag on a column without lineage is not unset when unsetAllowed is false") {
    val current = Map("first_col" -> Seq(Tag("blocked", "logistic_center")))
    val desired = Seq(plan("lc_col", Tag("logistic_center", "LC1")))

    val diff = guarded(desired, current, Seq("first_col", "lc_col"), withLineage = Set("lc_col"), unsetAllowed = false)

    diff.toUnset shouldBe empty
    diff.keptByMissingLineage shouldBe empty
    diff.toSet shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
  }

  test("a governed tag on a column without lineage is unset when another governed tag is desired elsewhere") {
    val current = Map("lc_col" -> Seq(Tag("logistic_center", "LC1")))
    val desired = Seq(plan("mc_col", Tag("management_company", "MC1")))

    val diff = guarded(desired, current, Seq("lc_col", "mc_col"), withLineage = Set("mc_col"))

    diff.toSet shouldBe Seq(change("mc_col", "management_company", "MC1"))
    diff.toUnset shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
    diff.keptByMissingLineage shouldBe empty
  }

  test("the same governed key moving to another column is unset from the old column without lineage") {
    val current = Map("old_col" -> Seq(Tag("logistic_center", "LC1")))
    val desired = Seq(plan("new_col", Tag("logistic_center", "LC1")))

    val diff = guarded(desired, current, Seq("old_col", "new_col"), withLineage = Set("new_col"))

    diff.toSet shouldBe Seq(change("new_col", "logistic_center", "LC1"))
    diff.toUnset shouldBe Seq(change("old_col", "logistic_center", "LC1"))
  }

  test("a governed tag on a column without lineage is kept when the desired state has no governed nor blocked") {
    val current = Map("lc_col" -> Seq(Tag("logistic_center", "LC1")))
    val desired = Seq(plan("other_col", Tag("PII", "true")))

    val diff = guarded(desired, current, Seq("lc_col", "other_col"), withLineage = Set("other_col"))

    diff.toUnset shouldBe empty
    diff.keptByMissingLineage shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
  }

  test("a desired state with only blocked lets the governed tag of a column without lineage be unset") {
    val current = Map("lc_col" -> Seq(Tag("logistic_center", "LC1")))
    val desired = Seq(plan("first_col", Tag("blocked", "logistic_center")))

    val diff = guarded(desired, current, Seq("first_col", "lc_col"), withLineage = Set("calc_col"))

    diff.toSet shouldBe Seq(change("first_col", "blocked", "logistic_center"))
    diff.toUnset shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
  }

  test("a non governed tag on a column without lineage is kept even if a governed tag is desired") {
    val current = Map("lc_col" -> Seq(Tag("PII", "true"), Tag("logistic_center", "LC1")))
    val desired = Seq(plan("mc_col", Tag("management_company", "MC1")))

    val diff = guarded(desired, current, Seq("lc_col", "mc_col"), withLineage = Set("mc_col"))

    diff.toUnset shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
    diff.keptByMissingLineage shouldBe Seq(change("lc_col", "PII", "true"))
  }

  test("a governed tag desired on a column that no longer exists does not lift the column guard") {
    val current = Map("lc_col" -> Seq(Tag("logistic_center", "LC1")))
    val desired = Seq(plan("dropped_col", Tag("management_company", "MC1")))

    val diff = guarded(desired, current, Seq("lc_col"), withLineage = Set.empty)

    diff.toSet shouldBe empty
    diff.toUnset shouldBe empty
    diff.keptByMissingLineage shouldBe Seq(change("lc_col", "logistic_center", "LC1"))
  }

  test("without governedKeys nor tableLevelKeys the column guard keeps blocked and governed tags") {
    val current = Map("first_col" -> Seq(Tag("blocked", "logistic_center")),
      "lc_col" -> Seq(Tag("logistic_center", "LC1")))
    val desired = Seq(plan("mc_col", Tag("management_company", "MC1")))

    val diff = TagSyncDiff.compute(desired, current, Seq("first_col", "lc_col", "mc_col"),
      columnsWithLineage = Some(Set("mc_col")))

    diff.toUnset shouldBe empty
    diff.keptByMissingLineage should have size 2
  }
}
