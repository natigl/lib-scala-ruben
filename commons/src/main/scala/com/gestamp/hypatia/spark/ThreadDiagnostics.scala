package com.gestamp.hypatia.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Diagnóstico temporal del contexto del hilo que ejecuta una lectura/escritura de Spark.
 *
 * Contexto: con parallelProcessing = true las entidades se procesan en hilos de un pool paralelo. En algunos
 * hilos la escritura falla con DATA_SOURCE_NOT_FOUND (tahoe) y en otros no, y las tablas con FGAC en dedicated
 * compute fallan con "No active Spark Session". Estas trazas permiten comparar, hilo a hilo, el context
 * classloader, las sesiones Spark visibles y los data sources que el hilo es capaz de resolver.
 *
 * Todas las líneas llevan el prefijo [DIAG-THREAD] para poder filtrarlas y retirarlas fácilmente.
 * Nunca lanza excepciones: cualquier fallo al recopilar datos se reporta en la propia línea.
 */
object ThreadDiagnostics {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val Prefix = "[DIAG-THREAD]"
  private val MaxClassLoaderDepth = 8
  private val MaxProviders = 200

  private def hash(o: AnyRef): String = if (o == null) "null" else f"${System.identityHashCode(o)}%08x"

  private def describe(cl: ClassLoader): String = if (cl == null) "null" else s"${cl.getClass.getName}@${hash(cl)}"

  private def loaderChain(cl: ClassLoader): String =
    Iterator.iterate(cl)(_.getParent).takeWhile(_ != null).take(MaxClassLoaderDepth).map(describe).mkString(" -> ")

  private def sessionHash(session: Option[SparkSession]): String = session.map(hash).getOrElse("none")

  /**
   * Nombres cortos de los data sources que el classloader indicado es capaz de resolver con ServiceLoader,
   * que es el mecanismo que usa Spark en DataSource.lookupDataSource.
   */
  private def visibleDataSources(cl: ClassLoader): Try[Seq[String]] = Try {
    val iterator = java.util.ServiceLoader.load(classOf[DataSourceRegister], cl).iterator()
    val names = ArrayBuffer.empty[String]
    var visited = 0
    var more = Try(iterator.hasNext).getOrElse(false)
    while (more && visited < MaxProviders) {
      Try(iterator.next().shortName()).foreach(names += _)
      visited += 1
      more = Try(iterator.hasNext).getOrElse(false)
    }
    names.toList
  }

  /**
   * Cadena de causas de una excepción, de la más externa a la raíz, para ver el error real sin la traza completa.
   */
  private def causeChain(t: Throwable): String =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(MaxClassLoaderDepth)
      .map(e => s"${e.getClass.getName}: ${e.getMessage}").mkString(" > ")

  /**
   * Registra el contexto del hilo actual: identidad, classloaders y sesiones Spark visibles.
   *
   * @param label     operación que se está ejecutando (p. ej. "write:catalog.schema.table")
   * @param dfSession SparkSession del DataFrame implicado, si existe
   */
  def logThreadContext(label: String, dfSession: Option[SparkSession] = None): Unit = {
    val line = Try {
      val thread = Thread.currentThread()
      val contextLoader = thread.getContextClassLoader
      val sparkLoader = classOf[SparkSession].getClassLoader
      val libLoader = this.getClass.getClassLoader

      val active = SparkSession.getActiveSession
      val default = SparkSession.getDefaultSession
      val library = Try(SparkConfiguration.spark).toOption

      val providers = visibleDataSources(contextLoader)
      val providersInfo = providers.map { names =>
        s"count=${names.size} tahoe=${names.contains("tahoe")} delta=${names.contains("delta")}"
      }.recover { case e => s"lookupFailed=${e.getClass.getName}: ${e.getMessage}" }.get

      s"$Prefix label=$label thread=${thread.getName}(id=${thread.getId}, group=${Option(thread.getThreadGroup).map(_.getName).orNull}, " +
        s"daemon=${thread.isDaemon}) | contextCL=${describe(contextLoader)} chain=[${loaderChain(contextLoader)}] " +
        s"| sparkCL=${describe(sparkLoader)} libCL=${describe(libLoader)} " +
        s"contextCL==sparkCL=${contextLoader eq sparkLoader} contextCL==libCL=${contextLoader eq libLoader} " +
        s"| activeSession=${sessionHash(active)} defaultSession=${sessionHash(default)} librarySession=${sessionHash(library)} " +
        s"dfSession=${sessionHash(dfSession)} active==library=${active.exists(a => library.exists(_ eq a))} " +
        s"| dataSourceProviders($providersInfo)"
    }.recover { case e => s"$Prefix label=$label diagnosticFailed=${e.getClass.getName}: ${e.getMessage}" }.get

    logger.info(line)
  }

  /**
   * Registra el fallo de una operación junto con el contexto del hilo en el que ocurrió.
   *
   * @param label     operación que ha fallado
   * @param error     excepción capturada
   * @param dfSession SparkSession del DataFrame implicado, si existe
   */
  def logFailure(label: String, error: Throwable, dfSession: Option[SparkSession] = None): Unit = {
    logger.error(s"$Prefix FAILED label=$label causes=[${causeChain(error)}]")
    logThreadContext(s"$label (after failure)", dfSession)
  }
}
