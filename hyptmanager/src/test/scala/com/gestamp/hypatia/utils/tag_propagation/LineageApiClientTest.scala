package com.gestamp.hypatia.utils.tag_propagation

import org.scalatest.{FunSuite, Matchers}

/**
 * Pure tests (no Spark, no HTTP) for [[LineageApiClient]]: parsing of the lineage-tracking REST responses and the
 * behaviour without host/token.
 */
class LineageApiClientTest extends FunSuite with Matchers {

  test("parseColumnUpstreams returns the upstream columns, without duplicates") {
    val json =
      """{"upstream_cols":[
        |  {"name":"logisticCenter_code","catalog_name":"semantic_lab","schema_name":"mat_mmd","table_name":"material_valuation"},
        |  {"name":"logisticCenter_code","catalog_name":"semantic_lab","schema_name":"mat_mmd","table_name":"material_valuation"},
        |  {"name":"plant","catalog_name":"semantic_lab","schema_name":"org_lcs","table_name":"organizational_mapping_master"}
        |],"downstream_cols":[]}""".stripMargin
    LineageApiClient.parseColumnUpstreams(json) shouldBe Seq(
      LineageSourceColumn("semantic_lab", "mat_mmd", "material_valuation", "logisticCenter_code"),
      LineageSourceColumn("semantic_lab", "org_lcs", "organizational_mapping_master", "plant"))
  }

  test("parseColumnUpstreams is empty without upstream_cols") {
    LineageApiClient.parseColumnUpstreams("{}") shouldBe empty
    LineageApiClient.parseColumnUpstreams("""{"upstream_cols":[]}""") shouldBe empty
  }

  test("parseColumnUpstreams skips entries without catalog, schema, table or column") {
    val json = """{"upstream_cols":[{"name":"c","catalog_name":"semantic_lab","table_name":"t"}]}"""
    LineageApiClient.parseColumnUpstreams(json) shouldBe empty
  }

  test("parseTableUpstreams returns catalog.schema.table of the upstream tables") {
    val json =
      """{"upstreams":[
        |  {"tableInfo":{"name":"material_valuation","catalog_name":"semantic_lab","schema_name":"mat_mmd","table_type":"TABLE"}},
        |  {"tableInfo":{"name":"organizational_mapping_master","catalog_name":"semantic_lab","schema_name":"org_lcs"}},
        |  {"notebookInfos":[{"workspace_id":1,"notebook_id":2}]}
        |]}""".stripMargin
    LineageApiClient.parseTableUpstreams(json) shouldBe Seq(
      "semantic_lab.mat_mmd.material_valuation", "semantic_lab.org_lcs.organizational_mapping_master")
  }

  test("parseTableUpstreams is empty without upstreams") {
    LineageApiClient.parseTableUpstreams("""{"downstreams":[]}""") shouldBe empty
  }

  /** Cliente con respuestas HTTP simuladas, en orden; cuenta las llamadas. */
  private class FakeClient(responses: Seq[LineageApiClient.HttpResult], maxRetries: Int = 3)
    extends LineageApiClient(Some("https://host"), Some("token"), maxRetries = maxRetries, retryBaseSleepMs = 1L) {
    var calls = 0
    override protected def fetch(url: String): scala.util.Try[LineageApiClient.HttpResult] = {
      val r = responses(math.min(calls, responses.size - 1)); calls += 1; scala.util.Success(r)
    }
  }

  private val okBody = """{"upstreams":[{"tableInfo":{"name":"t","catalog_name":"c","schema_name":"s"}}]}"""

  test("HTTP 429 and 503 are retried until the API answers") {
    val client = new FakeClient(Seq(LineageApiClient.HttpResult(429, "slow down"), LineageApiClient.HttpResult(503, ""),
      LineageApiClient.HttpResult(200, okBody)))
    client.tableUpstreams("c.s.g") shouldBe Some(Seq("c.s.t"))
    client.calls shouldBe 3
  }

  test("HTTP 429 gives None after maxRetries retries") {
    val client = new FakeClient(Seq(LineageApiClient.HttpResult(429, "slow down")), maxRetries = 2)
    client.tableUpstreams("c.s.g") shouldBe None
    client.calls shouldBe 3
  }

  test("other HTTP errors are not retried") {
    val client = new FakeClient(Seq(LineageApiClient.HttpResult(404, "not found"), LineageApiClient.HttpResult(200, okBody)))
    client.tableUpstreams("c.s.g") shouldBe None
    client.calls shouldBe 1
  }

  test("Retry-After of the response is honoured (seconds)") {
    val client = new FakeClient(Seq(LineageApiClient.HttpResult(429, "", Some(0L)), LineageApiClient.HttpResult(200, okBody)))
    client.tableUpstreams("c.s.g") shouldBe Some(Seq("c.s.t"))
    client.calls shouldBe 2
  }

  test("without host or token the client is not available and returns None without calling the API") {
    val noToken = new LineageApiClient(Some("https://host"), None)
    noToken.available shouldBe false
    noToken.columnUpstreams("c.s.t", "col") shouldBe None
    noToken.tableUpstreams("c.s.t") shouldBe None
    noToken.waitForTableLineage("c.s.t", attempts = 3, sleepMs = 1) shouldBe false
    new LineageApiClient(None, Some("token")).available shouldBe false
    new LineageApiClient(Some(""), Some("token")).available shouldBe false
  }
}
