package com.gestamp.hypatia.utils.tag_propagation

import org.scalatest.{FunSuite, Matchers}

/**
 * Tests puros (sin Spark) del tipo [[TableRef]]: la normalización de identificadores de Unity Catalog.
 */
class TableRefTest extends FunSuite with Matchers {

  test("apply pasa catálogo, esquema y tabla a minúsculas (como los guarda Unity Catalog)") {
    val ref = TableRef("HUMAN_Resources_Dev", "HR_Analytics", "HeadcountActual")
    ref.catalog shouldBe "human_resources_dev"
    ref.schema shouldBe "hr_analytics"
    ref.table shouldBe "headcountactual"
    ref.full shouldBe "human_resources_dev.hr_analytics.headcountactual"
  }

  test("fromFull parsea y normaliza un nombre catalogo.esquema.tabla") {
    TableRef.fromFull("Cat.Sch.HeadcountActual") shouldBe TableRef("cat", "sch", "headcountactual")
  }

  test("fromFull rechaza un nombre que no tiene exactamente tres partes") {
    an[IllegalArgumentException] should be thrownBy TableRef.fromFull("cat.schema")
    an[IllegalArgumentException] should be thrownBy TableRef.fromFull("cat.schema.table.extra")
  }

  test("equals y hashCode comparan los identificadores ya normalizados") {
    TableRef("Cat", "Sch", "Tbl") shouldBe TableRef("cat", "sch", "tbl")
    TableRef("Cat", "Sch", "Tbl").hashCode shouldBe TableRef("cat", "sch", "tbl").hashCode
  }

  test("normalize pasa a minúsculas y tolera null") {
    TableRef.normalize("HeadcountActual") shouldBe "headcountactual"
    TableRef.normalize(null) shouldBe null
  }
}
