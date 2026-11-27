package com.gestamp.hypatia.spark

import com.gestamp.hypatia.exceptions.SparkconfigurationException
import com.gestamp.hypatia.utils.JsonTools
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object SparkConfiguration extends SparkConfiguration

class SparkConfiguration {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val jsonStr: String = readSparkConfiguration()
  private val sparkMap = JsonTools.jsonStrToMap(jsonStr)
  private val AppliedMarkerKey = "hypatia.sparkconfig.applied"

  /*
  val spark: SparkSession = {
    val sparkConf = sparkMap.foldLeft(new SparkConf) {
      case (conf, ("master", value)) =>
        logger.info(s"Creating Spark Session with master: $value")
        conf.setMaster(value)
      case (conf, ("applicationName", value)) =>
        logger.info(s"Creating Spark Session with Application Name: $value")
        conf.setAppName(value)
      case (conf, (key, value)) =>
        logger.info(s"Creating Spark Session with property: $key and value: $value")
        conf.set(key, value)
    }

    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  */

  /**
   * Devuelve la SparkSession vigente con esta prioridad:
   *  1) SparkSessionHolder (sesión oficial de la task)
   *  2) ActiveSession (thread-local)
   *  3) DefaultSession (global)
   *  4) getOrCreate() del runtime
   *
   * Además aplica la configuración del JSON sobre la sesión ya existente (solo una vez),
   * sin reconstruir Spark con builder.config(SparkConf), lo cual es frágil en Databricks.
   */
  def spark: SparkSession = {
    val s = SparkSessionHolder.get
      .orElse(SparkSession.getActiveSession)
      .orElse(SparkSession.getDefaultSession)
      .getOrElse(SparkSession.builder().getOrCreate())

    val alreadyApplied = s.conf.get(AppliedMarkerKey, "false") == "true"

    // Aplica SparkConfig.json solo una vez por SparkSession (no por instancia de SparkConfiguration)
    if (!alreadyApplied) this.synchronized {
      val stillNotApplied = s.conf.get(AppliedMarkerKey, "false") != "true"

      if (stillNotApplied) {
        sparkMap.foreach {
          case ("master", value) =>
            // Master only for local executions.
            logger.info(s"Ignoring 'master' in Databricks: $value")

          case ("applicationName", value) =>
            // En Databricks, el nombre de app viene dado por el job/cluster.
            logger.info(s"Ignoring 'applicationName' in Databricks: $value")

          case (key, value) =>
            try {
              logger.info(s"Setting Spark conf: $key=$value")
              s.conf.set(key, value)
            } catch {
              case e: Exception =>
                logger.warn(s"Skipping Spark conf $key=$value due to error: ${e.getMessage}")
            }
        }

        // Marca esta sesion como ya configurada
        try {
          s.conf.set(AppliedMarkerKey, "true")
          logger.info(s"Setting Spark config applied marker '$AppliedMarkerKey':'true'")
        } catch {
          case e: Exception =>
            logger.warn(s"Could not set Spark config applied marker '$AppliedMarkerKey': ${e.getMessage}")
        }
      }
    }
    logger.debug(
      s"SparkConfiguration.spark resolved " +
        s"sparkHash=${System.identityHashCode(s)}, " +
        s"(active=${SparkSession.getActiveSession.map(System.identityHashCode)}, " +
        s"default=${SparkSession.getDefaultSession.map(System.identityHashCode)}) "
    )
    s
  }


/*  val sparkConf = sparkMap.keySet.foldLeft(new SparkConf())(
    (sparkConfig, key) => key match {
        case "master" => {
          logger.info(s"Creating Spark Session with master: ${sparkMap.get(key).get}")
          sparkConfig.setMaster(sparkMap.get(key).get)
        }
        case "applicationName" => {
          logger.info(s"Creating Spark Session with Application Name: ${sparkMap.get(key).get}")
          sparkConfig.setAppName(sparkMap.get(key).get)
        }
        case _ => {
          logger.info(s"Creating Spark Session with property: $key and value: ${sparkMap.get(key).get}")
          sparkConfig.set(key, sparkMap.get(key).get)
        }
  })

  lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()*/


  /**
   *
   * @param path
   * @return
   */
  @throws(classOf[SparkconfigurationException])
  private def readSparkConfiguration(path: String = ""): String = {

    if(path.nonEmpty) {
      logger.info(s"Read Spark Configuration from custom path: $path")
      val resource = Source.fromResource(path).getLines().mkString
      if(resource == null)
        throw new SparkconfigurationException(s"Configuration file not found")
      return resource
    }

    logger.info("Read Spark Configuration from local library: ../SparkConfig.json")
    val resource = Source.fromResource("SparkConfig.json").getLines().mkString
    if(resource == null)
      throw new SparkconfigurationException(s"Configuration file not found")

    resource
  }

}
