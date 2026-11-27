package com.gestamp.hypatia.databricks.deltalake

import com.gestamp.hypatia.exceptions.DeltaTableManagerException
import com.gestamp.hypatia.spark.SparkConfiguration
import com.gestamp.hypatia.utils.CommonsConstants
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Left, Right, Success, Try}


object DeltaChangeDataFeed extends IDeltaChangeDataFeed {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Method to activate the Change Data Feed to a Delta table using SQL language
   *
   * @param Deltatable
   * @throws com.gestamp.hypatia.exceptions.DeltaTableManagerException
   * @return
   */
  @throws(classOf[DeltaTableManagerException])
  override def ActiveChangeDataFeed(Deltatable: String): Either[Throwable, String] = {
    logger.info(s"CDF is Actived in delta table: $Deltatable")
    Try {
      SparkConfiguration.spark.sql(s"ALTER TABLE $Deltatable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    } match {
      case Success(_) => Right(Deltatable)
      case Failure(e) => Left(new DeltaTableManagerException(s"Error activing CDF on $Deltatable", e))
    }
  }

  /**
   * Method for get the latest version of a Delta Table by him ADLS Path
   *
   * @param deltaTablePath
   * @return
   */
  @throws(classOf[DeltaTableManagerException])
  override def getLatestDeltaVersion(deltaTablePath: String): Long = {
    logger.info(s"Start method to get latest version of table at path = $deltaTablePath")

    val deltaTable = DeltaTable.forPath(SparkConfiguration.spark, deltaTablePath)

    Try(deltaTable.history(1).select("version").collect()(0)(0).asInstanceOf[Long]) match {
      case Success(latestVersion) =>
        logger.info(s"Latest version of table ($deltaTablePath) = $latestVersion")
        latestVersion
      case Failure(e) =>
        throw new DeltaTableManagerException(s"Error determining the latest version of the Delta table at $deltaTablePath", e)
    }
  }

  /**
   * Devuelve la última versión "significativa" de la tabla: busca en las últimas 30 entradas
   * del historial la operación más reciente que modifica datos (WRITE, MERGE, UPDATE, DELETE,
   * CTAS, CLONE...), ignorando mantenimiento tipo OPTIMIZE/VACUUM.
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @return el número de la última versión con cambios de datos
   * @throws DeltaTableManagerException si no se puede determinar la versión
   */
  override def getLatestSignificantDeltaVersion(catalog: String, schema: String, deltaObj: String): Long = {
    logger.info(s"Start method to get latest meaningful delta version of $catalog.$schema.$deltaObj table")

    val deltaTable = DeltaTable.forName(SparkConfiguration.spark, s"$catalog.$schema.$deltaObj")

    val meaningfulOps = Seq("WRITE", "CREATE OR REPLACE TABLE AS SELECT","CREATE TABLE AS SELECT", "MERGE", "UPDATE", "DELETE", "CREATE OR REPLACE", "CLONE")

    Try {
      val historyDF = deltaTable.history(30)

      val latestMeaningfulVersion = historyDF
        .filter(col("operation").isin(meaningfulOps: _*))
        .select("version")
        .orderBy(col("version").desc)
        .limit(1)
        .collect()
        .head
        .getAs[Long]("version")

      latestMeaningfulVersion
    } match {
      case Success(version) =>
        logger.info(s"Latest meaningful version of $catalog.$schema.$deltaObj Delta table: $version")
        version
      case Failure(e) =>
        throw new DeltaTableManagerException(
          s"Error determining the latest meaningful version of $catalog.$schema.$deltaObj Delta table: ${e.getMessage}",
          e
        )
    }
  }

  /**
   * Lee del Change Data Feed los cambios de la última versión de la tabla quedándose
   * solo con las operaciones 'insert' (escenario de extracciones FULL/append).
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @param path     ruta de la tabla, usada para resolver la última versión
   * @return Right con el DataFrame de cambios, o Left con el error
   */
  override def readDeltaFeedFromLastVersion(catalog: String, schema: String, deltaObj: String, path: String): Either[Throwable, DataFrame] = {
    logger.info(s"Reading Delta table latest version (only 'insert' operation): $catalog.$schema.$deltaObj")

    Try(SparkConfiguration.spark.read.format(CommonsConstants.DELTA_FORMAT)
      .option("readChangeFeed", "true")
      .option("startingVersion", getLatestDeltaVersion(path))
      .table(s"$catalog.$schema.$deltaObj")
      .filter(col("_change_type").equalTo("insert"))) match {
      case Success(df) =>
        logger.info(s"Read latest version of Delta Table $catalog.$schema.$deltaObj success!")
        Right(df.toDF)
      case Failure(exception) =>
        Left(new DeltaTableManagerException(s"Error reading the latest version of the Delta table at $catalog.$schema.$deltaObj}\n" +
          s"Error detail: ${exception.getMessage}", exception))
    }
  }

  /**
   * Reads the latest version of a Delta table with 'insert', 'update', 'delete' operations.
   * Escenarios cubiertos (insert, update & delete):
   *      - Extracciones CDC
   *
   *  @param catalog the catalog the Delta table belongs to
   *  @param schema the schema the Delta table belongs to
   *  @param deltaObj the name of the Delta table
   *  @param path the path of the Delta table
   *  @return a DataFrame of the latest version of the Delta table with only 'insert' operation.
   *  @throws DeltaTableManagerException in case of any error.
   */
  override def readDeltaFeedFromLastVersionCDC(catalog: String, schema: String, deltaObj: String, path: String): Either[Throwable, DataFrame] = {
    logger.info(s"Reading Delta table latest version (CDC operations): $catalog.$schema.$deltaObj")

    Try(SparkConfiguration.spark.read.format(CommonsConstants.DELTA_FORMAT)
      .option("readChangeFeed", "true")
      .option("startingVersion", getLatestDeltaVersion(path))
      .table(s"$catalog.$schema.$deltaObj")
      .filter(col("_change_type").notEqual("update_preimage"))) match {
      case Success(df) =>
        logger.info(s"Read latest version of Delta Table $catalog.$schema.$deltaObj success!")
        Right(df.toDF)
      case Failure(exception) =>
        Left(new DeltaTableManagerException(s"Error reading the latest version of the Delta table at $catalog.$schema.$deltaObj}\n" +
          s"Error detail: ${exception.getMessage}", exception))
    }
  }


  /**
   * Lee del Change Data Feed los cambios de una versión concreta (startingVersion =
   * endingVersion), quedándose solo con las operaciones 'insert'.
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @param version  versión a leer
   * @return Right con el DataFrame de cambios, o Left con el error
   */
  override def readDeltaFeedFromVersion(catalog: String, schema: String, deltaObj: String, version: Long): Either[Throwable, DataFrame] = {
    logger.info(s"Reading '$version' delta version (only 'insert' operation): $catalog.$schema.$deltaObj")

    Try(SparkConfiguration.spark.read.format(CommonsConstants.DELTA_FORMAT)
      .option("readChangeFeed", "true")
      .option("startingVersion", version)
      .option("endingVersion", version)
      .table(s"$catalog.$schema.$deltaObj")
      .filter(col("_change_type").equalTo("insert"))) match {
      case Success(df) =>
        logger.info(s"Version reading of $catalog.$schema.$deltaObj Delta table ended successfully!")
        Right(df.toDF)
      case Failure(exception) =>
        Left(new DeltaTableManagerException(s"Error reading the version $version of $catalog.$schema.$deltaObj Delta table\n" +
          s"Error detail: ${exception.getMessage}", exception))
    }
  }

  /**
   * Lee del Change Data Feed los cambios de una versión concreta incluyendo operaciones
   * CDC (insert, update y delete; se excluye update_preimage).
   *
   * @param catalog  catálogo que contiene la tabla
   * @param schema   esquema que contiene la tabla
   * @param deltaObj nombre de la tabla
   * @param version  versión a leer
   * @return Right con el DataFrame de cambios, o Left con el error
   */
  override def readDeltaFeedFromVersionCDC(catalog: String, schema: String, deltaObj: String, version: Long): Either[Throwable, DataFrame] = {
    logger.info(s"Reading '$version' delta version (CDC operation): $catalog.$schema.$deltaObj")

    Try(SparkConfiguration.spark.read.format(CommonsConstants.DELTA_FORMAT)
      .option("readChangeFeed", "true")
      .option("startingVersion", version)
      .option("endingVersion", version)
      .table(s"$catalog.$schema.$deltaObj")
      .filter(col("_change_type").notEqual("update_preimage"))) match {
      case Success(df) =>
        logger.info(s"Version reading of $catalog.$schema.$deltaObj Delta table ended successfully!")
        Right(df.toDF)
      case Failure(exception) =>
        Left(new DeltaTableManagerException(s"Error reading the version $version of $catalog.$schema.$deltaObj Delta table (CDC)\n" +
          s"Error detail: ${exception.getMessage}", exception))
    }
  }


  /**
   * Generate join condition string using the key columns
   * @param keyColumns
   * @param targetAlias
   * @param sourceAlias
   * @return
   */
  private def genJoinCondition(keyColumns: Seq[String], targetAlias: String, sourceAlias: String): String = {
    val joinCondition = keyColumns.map { key =>
      s"$targetAlias.$key = $sourceAlias.$key"
    }.mkString(" AND ")
    logger.info(s"Join Condition: $joinCondition")

    joinCondition
  }

  /**
   * Generates the merge expression map for use in the Delta merge statement
   * @param dfSource
   * @param sourceAlias
   * @return
   */
  private def genMergeExpressions(dfSource: DataFrame, sourceAlias: String, columnsNames: Set[String]): Map[String, String] = {
    if (columnsNames("_SEQUENCENUMBER")) {
      val mergeExpressions = dfSource.columns.filterNot(Seq("ODQ_CHANGEMODE", "ODQ_ENTITYCNTR", "_PACKAGEID", "_SEQUENCENUMBER", "_change_type", "_commit_version", "_commit_timestamp", "_rescued_data", "source_metadata")
        .contains(_)).map { column => s"`$column`" -> s"$sourceAlias.`$column`"
      }.toMap
      logger.info(s"Merge Expression: $mergeExpressions")
      mergeExpressions
    } else {
      val mergeExpressions = dfSource.columns.filterNot(Seq("GLREQUEST","GLDELFLAG","GLCDCFLAG", "GLCHANGETIME", "_change_type", "_commit_version", "_commit_timestamp", "_rescued_data", "source_metadata")
        .contains(_)).map { column => s"`$column`" -> s"$sourceAlias.`$column`"
      }.toMap
      logger.info(s"Merge Expression: $mergeExpressions")
      mergeExpressions
    }
  }

  /**
   * Deduce la expresión de borrado del merge según las columnas presentes en el origen:
   * ODQ_CHANGEMODE == 'D' (SAP ODQ) o _change_type == 'delete' (CDF); None si no hay marca de borrado.
   *
   * @param columnsNames conjunto de nombres de columnas del DataFrame origen
   * @return la expresión de filtro de borrados, si aplica
   */
  private def genDeleteFilterExpression(columnsNames: Set[String]) = {
    if (columnsNames("ODQ_CHANGEMODE")) Some("ODQ_CHANGEMODE == 'D'")
    else if (columnsNames("_change_type")) Some("_change_type == 'delete'")
    else if (columnsNames("GLCDCFLAG")) Some("GLCDCFLAG == 'D'")
    else None
  }

  /**
   * Performs an custom upsert operation on a Delta table at the silver level. This function updates existing records,
   * inserts new records, and deletes records from the target Delta table based on the given DataFrame and key columns.
   *
   * @param deltaTarget The target Delta table to perform the upsert operation on.
   * @param dfSource    The source DataFrame containing the records to be upserted.
   * @param keyColumns  A sequence of key column names used to define the join condition for merging the target and source tables.
   * @return Either a Throwable error or a Unit indicating the success of the upsert operation.
   * @throws DeltaTableManagerException If an error occurs during the merge operation.
   */
  @throws(classOf[DeltaTableManagerException])
  override def upsertDeltaTableChangeDataCapture(deltaTarget: DeltaTable, dfSource: DataFrame, keyColumns: Seq[String]): Either[Throwable, Unit] = {
    logger.info(s"Starting upsert Quality operation on Delta table $deltaTarget")

    val targetAlias = "target"
    val sourceAlias = "source"
    val columnsNames = dfSource.columns.toSet

    val joinCondition = genJoinCondition(keyColumns, targetAlias, sourceAlias)
    val mergeExpressions = genMergeExpressions(dfSource, sourceAlias, columnsNames)


    val filterDelete = genDeleteFilterExpression(columnsNames)

    logger.info("Upsert!@")
    logger.info(s"Columns source: $columnsNames")
    logger.info(s"Join Condition: $joinCondition")
    logger.info(s"Merge Expression: $mergeExpressions")
    logger.info(s"Filter Delete: $filterDelete")

    val deltaMergeBuilder = filterDelete match {
      case Some(expression) =>
        logger.info(s"Delete expression detected: $expression")
        deltaTarget.alias(targetAlias)
          .merge(dfSource.alias(sourceAlias), joinCondition)
          .whenMatched(s"$sourceAlias.$expression").delete()
          .whenMatched().updateExpr(mergeExpressions)
          .whenNotMatched(s"NOT $sourceAlias.$expression").insertExpr(mergeExpressions)
      case None =>
        logger.info(s"There´s no delete expression detected")
        deltaTarget.alias(targetAlias)
          .merge(dfSource.alias(sourceAlias), joinCondition)
          .whenMatched().updateExpr(mergeExpressions)
          .whenNotMatched().insertExpr(mergeExpressions)
    }

    Try(deltaMergeBuilder.execute()) match {
      case Success(value) =>
        logger.info(s"Merge Delta Table finished successfully!")
        Right(value)
      case Failure(e) => throw new DeltaTableManagerException(s"Error merging data: ${e.getMessage}", e)
    }
  }

}
