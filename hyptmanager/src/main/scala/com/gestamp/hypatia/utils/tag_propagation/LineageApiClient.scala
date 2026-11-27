package com.gestamp.hypatia.utils.tag_propagation

import com.databricks.dbutils_v1.DBUtilsHolder
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import java.io.InputStream
import java.net.{HttpURLConnection, URL, URLEncoder}
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Columna origen devuelta por la API de linaje.
 */
case class LineageSourceColumn(catalog: String, schema: String, tableName: String, columnName: String)

/**
 * Cliente de la API REST de linaje de Unity Catalog (/api/2.0/lineage-tracking).
 *
 * La tabla system.access.column_lineage llega con horas de retraso, de modo que una tabla Gold recién creada no tiene
 * linaje en ella y se queda sin tags (y por tanto sin row filter) hasta una ejecución posterior. La API REST refleja el
 * linaje de la escritura en segundos o minutos, por eso se consulta primero.
 *
 * Host y token salen del contexto de dbutils del job. Si no están disponibles o la llamada falla, los métodos devuelven
 * None y quien llama sigue con system.access.column_lineage.
 */
class LineageApiClient(apiUrl: Option[String], apiToken: Option[String], timeoutMs: Int = 20000,
                       maxRetries: Int = 5, retryBaseSleepMs: Long = 2000L) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /** Indica si hay host y token para llamar a la API. */
  def available: Boolean = apiUrl.exists(_.nonEmpty) && apiToken.exists(_.nonEmpty)

  /**
   * Columnas origen directas de una columna.
   *
   * @return Some(columnas) si la API responde (puede ser vacío), None si no se puede usar o falla
   */
  def columnUpstreams(fullTableName: String, columnName: String): Option[Seq[LineageSourceColumn]] =
    get("column-lineage", Map("table_name" -> fullTableName, "column_name" -> columnName))
      .map(LineageApiClient.parseColumnUpstreams)

  /**
   * Tablas origen directas de una tabla (catalogo.esquema.tabla).
   *
   * @return Some(tablas) si la API responde (puede ser vacío), None si no se puede usar o falla
   */
  def tableUpstreams(fullTableName: String): Option[Seq[String]] =
    get("table-lineage", Map("table_name" -> fullTableName, "include_entity_lineage" -> "false"))
      .map(LineageApiClient.parseTableUpstreams)

  /**
   * Espera a que la API tenga linaje de la tabla (lo publica unos segundos después de la escritura).
   *
   * @return true si aparece linaje dentro de los intentos, false si no aparece o la API no se puede usar
   */
  def waitForTableLineage(fullTableName: String, attempts: Int, sleepMs: Long): Boolean = {
    var i = 0
    while (i < attempts) {
      tableUpstreams(fullTableName) match {
        case None => return false
        case Some(ups) if ups.nonEmpty =>
          consoleLog(s"[LineageApi] $fullTableName — table lineage available after ${i + 1} attempt(s): ${ups.mkString(", ")}")
          return true
        case Some(_) =>
          i += 1
          if (i < attempts) {
            consoleLog(s"[LineageApi] $fullTableName — no table lineage yet, retry $i/$attempts in ${sleepMs}ms")
            Thread.sleep(sleepMs)
          }
      }
    }
    consoleLog(s"[LineageApi] $fullTableName — no table lineage after $attempts attempt(s)")
    false
  }

  private def get(endpoint: String, params: Map[String, String]): Option[String] = {
    if (!available) return None
    val query = params.map { case (k, v) => s"${URLEncoder.encode(k, "UTF-8")}=${URLEncoder.encode(v, "UTF-8")}" }.mkString("&")
    val url = s"${apiUrl.get.stripSuffix("/")}/api/2.0/lineage-tracking/$endpoint?$query"
    val what = s"$endpoint ${params.mkString(", ")}"
    var attempt = 0
    while (true) {
      fetch(url) match {
        case Success(r) if r.code < 300 => return Some(r.body)
        // 429/503: límite de peticiones de la API (varias tablas se procesan en paralelo). Se reintenta con
        // espera creciente; sin esto la columna caía a system.access.column_lineage, vacía en una tabla nueva.
        case Success(r) if (r.code == 429 || r.code == 503) && attempt < maxRetries =>
          val sleepMs = r.retryAfterSeconds.map(_ * 1000L)
            .getOrElse(retryBaseSleepMs * (1L << attempt) + (scala.util.Random.nextDouble() * retryBaseSleepMs).toLong)
          attempt += 1
          consoleLog(s"[LineageApi] $what -> HTTP ${r.code}, retry $attempt/$maxRetries in ${sleepMs}ms")
          Thread.sleep(sleepMs)
        case Success(r) =>
          consoleLog(s"[LineageApi] $what -> HTTP ${r.code}: ${r.body.take(300)}")
          return None
        case Failure(e) =>
          consoleLog(s"[LineageApi] $what failed: ${e.getMessage}")
          return None
      }
    }
    None
  }

  // Log y stdout: en el job JAR de USAFederated el log de la aplicación llega incompleto a la salida del run.
  private def consoleLog(msg: String): Unit = { logger.info(msg); println(msg) }

  /** Hace el GET (separado para poder probar los reintentos sin HTTP). */
  protected def fetch(url: String): Try[LineageApiClient.HttpResult] = Try {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod("GET")
      conn.setConnectTimeout(timeoutMs)
      conn.setReadTimeout(timeoutMs)
      conn.setRequestProperty("Authorization", s"Bearer ${apiToken.get}")
      conn.setRequestProperty("Accept", "application/json")
      val code = conn.getResponseCode
      val body = read(if (code >= 400) conn.getErrorStream else conn.getInputStream)
      val retryAfter = Option(conn.getHeaderField("Retry-After")).flatMap(h => Try(h.trim.toLong).toOption)
      LineageApiClient.HttpResult(code, body, retryAfter)
    } finally conn.disconnect()
  }

  private def read(is: InputStream): String =
    if (is == null) "" else { val s = Source.fromInputStream(is, "UTF-8"); try s.mkString finally s.close() }
}

object LineageApiClient {

  /** Respuesta HTTP: código, cuerpo y cabecera Retry-After en segundos si viene. */
  case class HttpResult(code: Int, body: String, retryAfterSeconds: Option[Long] = None)

  /** Cliente con host y token del contexto de dbutils del job (vacío si no hay contexto). */
  def fromDbutilsContext(): LineageApiClient = {
    // catch Throwable: sin dbutils en el classpath (tests locales) salta NoClassDefFoundError, que Try no captura
    val ctx = try Some(DBUtilsHolder.dbutils.notebook.getContext()) catch { case _: Throwable => None }
    new LineageApiClient(ctx.flatMap(_.apiUrl), ctx.flatMap(_.apiToken))
  }

  /** Parsea la respuesta de /column-lineage: upstream_cols[{catalog_name, schema_name, table_name, name}]. */
  def parseColumnUpstreams(json: String): Seq[LineageSourceColumn] = {
    implicit val formats: Formats = DefaultFormats
    (parse(json) \ "upstream_cols") match {
      case JArray(cols) => cols.flatMap { c =>
        for {
          cat <- (c \ "catalog_name").extractOpt[String]
          sch <- (c \ "schema_name").extractOpt[String]
          tbl <- (c \ "table_name").extractOpt[String]
          col <- (c \ "name").extractOpt[String]
        } yield LineageSourceColumn(cat, sch, tbl, col)
      }.distinct
      case _ => Seq.empty
    }
  }

  /** Parsea la respuesta de /table-lineage: upstreams[{tableInfo{catalog_name, schema_name, name}}]. */
  def parseTableUpstreams(json: String): Seq[String] = {
    implicit val formats: Formats = DefaultFormats
    (parse(json) \ "upstreams") match {
      case JArray(ups) => ups.flatMap { u =>
        val t = u \ "tableInfo"
        for {
          cat <- (t \ "catalog_name").extractOpt[String]
          sch <- (t \ "schema_name").extractOpt[String]
          tbl <- (t \ "name").extractOpt[String]
        } yield s"$cat.$sch.$tbl"
      }.distinct
      case _ => Seq.empty
    }
  }
}
