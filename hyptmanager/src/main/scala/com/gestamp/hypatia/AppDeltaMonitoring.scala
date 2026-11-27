package com.gestamp.hypatia

import com.gestamp.hypatia.spark.SparkConfiguration
import com.gestamp.hypatia.postgresql.dao.UsecaseDao
import com.gestamp.hypatia.args.{DeltaMonitoringParserArgs, JobParamsDeltaMonitoring}
import org.slf4j.{Logger, LoggerFactory}
import com.gestamp.hypatia.utils.CommonsConstants
import com.gestamp.hypatia.utils.HypatiaManagerConstants.{CATALOG_MONITORING, SCHEMA_MONITORING, TARGET_TABLE_MONITORING_GOLD}
import com.gestamp.hypatia.utils.monitoring.{DeltaMonitoringManager, DeltaMonitoringOrchestrator}

object AppDeltaMonitoring {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Main del job de monitorización Delta: resuelve el caso de uso desde los
   * argumentos, monitoriza todas las tablas de su catálogo.esquema y anexa los
   * resultados a la tabla de monitorización de gold.
   *
   * @param args argumentos de línea de comandos (id del proceso de Hypatia)
   */
  def main(args: Array[String]): Unit = {
    val jobParams: JobParamsDeltaMonitoring = DeltaMonitoringParserArgs.parserArgs(args)

    logger.info(s"Starting Delta Monitoring with HypatiaManagerProcess ID: ${jobParams.idHypatiaManagerProcess}")

    val usecase = jobParams.usecase.getOrElse(
      throw new RuntimeException("No se pudo obtener el Usecase desde los parámetros.")
    )

    val catalog = usecase.area + CommonsConstants.ENV_SOURCE
    val schema = usecase.name

    logger.info(s"Targeting catalog: $catalog, schema: $schema")

    val orchestrator = new DeltaMonitoringOrchestrator(new DeltaMonitoringManager())
    val results = orchestrator.execute(catalog, schema)

    val spark = SparkConfiguration.spark
    import spark.implicits._
    val df = results.toDF()

    val fullTableName = s"$CATALOG_MONITORING${CommonsConstants.ENV_SOURCE}.$SCHEMA_MONITORING.$TARGET_TABLE_MONITORING_GOLD"
    logger.info(s"Writing results to Delta table: $fullTableName (mode = append)")
    df.write
      .format("delta")
      .mode("append")
      .saveAsTable(fullTableName)
  }

}
