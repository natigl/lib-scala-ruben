package com.gestamp.hypatia.databricks.deltalake

import com.gestamp.hypatia.adls.ADLS2Manager.sparkWriteData
import com.gestamp.hypatia.databricks.deltalake.DeltaChangeDataFeed.ActiveChangeDataFeed
import com.gestamp.hypatia.exceptions.DeltaTableManagerException
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import org.apache.spark.sql.SparkSession
import com.gestamp.hypatia.utils.CommonsConstants
import com.gestamp.hypatia.utils.CommonsConstants.ENV
import io.delta.tables._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Left, Right, Success, Try}

object DeltaTablesManager extends IDeltaTablesManager {

  /**
   * Ejecuta el bloque con la SparkSession de la librería re-vinculada como sesión activa del hilo.
   * Necesario porque en Databricks (JAR task + REPL + reutilización de cluster) el código puede
   * ejecutarse en otro hilo con una ActiveSession thread-local obsoleta de una tarea anterior.
   *
   * @param f bloque a ejecutar con la sesión ya vinculada
   * @return el resultado del bloque
   */
  private def withBoundSparkSession[T](f: SparkSession => T): T = {
    val s = SparkConfiguration.spark
    SparkSession.clearActiveSession()
    SparkSession.setDefaultSession(s)
    SparkSession.setActiveSession(s)
    f(s)
  }


  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Creates a new database in DBFS if it does not exist.
   *
   * @param database the name of the database to be created
   * @return an Either object, with the Left value being a DeltaTableManagerException in case of failure,
   *         and the Right value being a string representing the database name in case of success
   * @throws DeltaTableManagerException if there is an error creating the database
   */
  @throws(classOf[DeltaTableManagerException])
  override def createDatabase(database: String): Either[Throwable, String] = {
    logger.info(s"Create new Database in DBFS if not exists: $database")
    Try(SparkConfiguration.spark.sql(s"CREATE SCHEMA IF NOT EXISTS $database")) match {
      case Success(_) =>
        changePermissionsTeamAdmins("SCHEMA", database) match {
          case Right(_) => Right(database)
          case Left(e) => Left(e)
        }
      case Failure(e) => Left(new DeltaTableManagerException(s"Error creating the database $database", e))
    }
  }

  /**
   * Creates a Delta table with the given name and location, and optional schema.
   *
   * If the schema is not provided, the table is created without any columns.
   *
   * @param table    the name of the table to be created
   * @param location the location where the table will be stored
   * @param schema   the schema of the table to be created
   * @return an Either object, with the Left value being a DeltaTableManagerException in case of failure,
   *         and the Right value being a string representing the table name in case of success
   * @throws DeltaTableManagerException if there is an error creating the table
   */
  @throws(classOf[DeltaTableManagerException])
  override def createDeltaTable(table: String, location: String, schema: StructType = StructType(Nil)): Either[Throwable, String] = {
    logger.info(s"Start create Delta Table '$table' using delta location $location")
    val dt = DeltaTable.createIfNotExists(SparkConfiguration.spark).location(location).tableName(table)
    val result = if (schema.isEmpty) Try(dt.execute())
    else Try(dt.addColumns(schema))
    result match {
      case Success(_) =>
        changePermissionsTeamAdmins("TABLE", table)
        ActiveChangeDataFeed(table)
      case Failure(e) => Left(new DeltaTableManagerException(s"Error creating the Delta Table '$table' using the location $location using the squema $schema", e))
    }
  }

  /**
   * Optimizes the specified Delta table.
   *
   * @param location The name of the Delta table to optimize.
   * @param field    The field to use for Z-ordering during optimization. If empty, a regular compaction will be performed.
   * @throws DeltaTableManagerException if there is an error optimizing the Delta table.
   * @return Either a string representing the name of the optimized table, or a throwable if there was an error.
   */
  @throws(classOf[DeltaTableManagerException])
  override def optimizeDeltaTable(location: String, field: String = null): Either[Throwable, String] = {
    val dt = DeltaTable.forPath(SparkConfiguration.spark, location).optimize()
    val result = if (field.isEmpty) {
      logger.info(s"Start optimize Delta Table '$location'")
      Try(dt.executeCompaction())
    } else {
      logger.info(s"Start optimize Delta Table '$location' with ZORDER BY: " + field)
      Try(dt.executeZOrderBy(field))
    }

    result match {
      case Success(_) => Right(location)
      case Failure(e) => Left(new DeltaTableManagerException(s"Error optimizing the Delta Table '$location'", e))
    }
  }

  /**
   * Cleans up old snapshots in the given Delta table.
   *
   * @param table the name of the table to clean up old snapshots from
   * @return an Either object, with the Left value being a DeltaTableManagerException in case of failure,
   *         and the Right value being a boolean indicating success in case of success
   * @throws DeltaTableManagerException if there is an error cleaning up old snapshots in the table
   */
  @throws(classOf[DeltaTableManagerException])
  override def vacuumDeltaTable(table: String): Either[Throwable, Boolean] = {
    logger.info(s"Start clean up old snapshot for Delta Table '$table'")
    val dt = DeltaTable.forPath(SparkConfiguration.spark, table)
    Try(dt.vacuum()) match {
      case Success(_) => Right(true)
      case Failure(e) => Left(new DeltaTableManagerException(s"Error cleaning old snapshots in the Delta Table '$table'", e))
    }
  }

  /**
   * Reverts the given Delta table to the specified version.
   *
   * @param path    the path of the table to be restored
   * @param version the version of the table to be restored
   * @return an Either object, with the Left value being a DeltaTableManagerException in case of failure,
   *         and the Right value being a boolean indicating success in case of success
   * @throws DeltaTableManagerException if there is an error reverting the table to the specified version
   */
  @throws(classOf[DeltaTableManagerException])
  override def revertToWriteDeltaTable(path: String, version: Long): Either[Throwable, Boolean] = {
    logger.info(s"Start reversing $path for version'$version'")
    Try(DeltaTable.forPath(SparkConfiguration.spark, path).restoreToVersion(version)) match {
      case Success(_) => Right(true)
      case Failure(e) => Left(new DeltaTableManagerException(s"Error reversing in the Delta Table '$path' for version'$version'", e))
    }
  }

  /**
   * Obtains detailed information about a given Delta table.
   *
   * @param path The path to the Delta table.
   * @throws DeltaTableManagerException If an error occurs while obtaining the details of the table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def detailDeltaTable(path: String) = {
    logger.info(s"Details table: $path")
    val deltaTable = DeltaTable.forPath(SparkConfiguration.spark, path)
    val detailDF = deltaTable.detail()

    detailDF.show()
  }

  /**
   *
   * @param viewName = <catalog_name>.<schema_name>.<view_name>
   * @param tempSparkView
   * @return
   */
  @throws(classOf[DeltaTableManagerException])
  override def createView(viewName: String, tempSparkView: String): Either[Throwable, String] = {
    logger.info(s"Create new Quality View in Delta Lake: $viewName")
    Try(SparkConfiguration.spark.sql(s"CREATE OR REPLACE VIEW IF NOT EXISTS $viewName AS SELECT * FROM $tempSparkView")) match {
      case Success(_) => Right(viewName)
      case Failure(e) => Left(new DeltaTableManagerException(s"Error creating the view $viewName", e))
    }
  }

  /**
   * Alter method to change ownership of delta objects
   *
   * @param securableType
   * @param securableName
   * @return
   */
    @throws(classOf[DeltaTableManagerException])
    override def changePermissionsTeamAdmins(securableType: String, securableName: String): Either[Throwable, String] = {
      logger.info(s"Start method to change owner object in environment = $ENV")
      Try{ if ( ENV.equals("dev") ) {
            logger.info(s"Permissions are changed to hub engineers: $securableType, $securableName")
            SparkConfiguration.spark.sql(s"ALTER $securableType $securableName OWNER TO `Access_HUB_Engineer`;")
          } else {
            logger.info(s"Permissions are changed to Access_HUB_Architect: $securableType, $securableName")
            SparkConfiguration.spark.sql(s"ALTER $securableType $securableName OWNER TO `Access_HUB_Architect`;")
          } } match {
        case Success(_) => Right(securableName)
        case Failure(e) => Left(new DeltaTableManagerException(s"Error changing permissions on $securableName\n${e.getMessage}", e))
      }
    }

  /**
   * Reads all data from a Delta Lake table using a catalog, schema, and delta object as arguments.
   *
   * @param catalog  The catalog containing the Delta Lake table.
   * @param schema   The schema containing the Delta Lake table.
   * @param deltaObj The name of the Delta Lake table to read.
   * @return A DataFrame representing the content of the Delta Lake table.
   * @throws Exception if an error occurs while reading the Delta Lake table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def readDeltaTable(catalog: String, schema: String, deltaObj: String): Either[Throwable, DataFrame] = {
    logger.info(s"Read Delta Table: $catalog.$schema.$deltaObj")

    // Databricks JAR task + REPL + cluster reuse can execute some code paths on a different thread.
    // We re-bind the Quality SparkSession here to avoid stale thread-local ActiveSession from previous tasks (e.g. ingestion).
    withBoundSparkSession { spark =>
      Try(DeltaTable.forName(s"$catalog.$schema.$deltaObj")) match {
        case Success(deltaTableResult) =>
          logger.info(s"Delta table casting to DF")
          Right(deltaTableResult.toDF)
        case Failure(exception) =>
          Left(new DeltaTableManagerException(s"Error reading the entire Delta table at $catalog.$schema.$deltaObj}\n" +
            s"Error detail: ${exception.getMessage}", exception))
      }
    }
  }

  /**
   * Delete data from a Delta Lake table using a catalog, schema, delta object and delete condition as arguments.
   *
   * @param catalog  The catalog containing the Delta Lake table.
   * @param schema   The schema containing the Delta Lake table.
   * @param deltaObj The name of the Delta Lake table to read.
   * @param condition The condition to delete data. Can be empty or specified.
   * @return A DataFrame representing the content of the Delta Lake table.
   * @throws Exception if an error occurs while looking for Delta Lake table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def deleteDeltaTable(catalog: String, schema: String, deltaObj: String, condition: String = ""): Either[Throwable, Unit] = {
    logger.info(s"Delete Delta Table: $catalog.$schema.$deltaObj")

    Try(DeltaTable.forName(s"$catalog.$schema.$deltaObj")) match {
      case Success(deltaTableResult) =>
        logger.info(s"Deleting Delta table")
        if (condition.isEmpty) {
          logger.info(s"Deleting all rows of Delta table")
          Right(deltaTableResult.delete())
        } else {
          logger.info(s"Deleting rows of Delta table under $condition condition")
          Right(deltaTableResult.delete(condition))
        }
      case Failure(exception) =>
        Left(new DeltaTableManagerException(s"Error trying to find the Delta table at $catalog.$schema.$deltaObj", exception))
    }
  }

  /**
   * Set properties for a Delta Lake table using a catalog, schema, delta object and properties as arguments.
   *
   * @param catalog   The catalog containing the Delta Lake table.
   * @param schema    The schema containing the Delta Lake table.
   * @param deltaObj  The name of the Delta Lake table to read.
   * @param properties A sequence containing the properties to be set.
   * @return Nothing (Unit)
   * @throws Exception if an error occurs while setting the properties for Delta Lake table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def setPropertiesDeltaTable(catalog: String, schema: String, deltaObj: String, properties: List[String]): Either[Throwable, Unit] = {
    logger.info(s"Setting the following properties for $catalog.$schema.$deltaObj Delta table: ${properties.mkString(",")}")
    Try(SparkConfiguration.spark.sql(
      s"""ALTER TABLE $catalog.$schema.$deltaObj
         |SET TBLPROPERTIES (${properties.mkString(",")})""".stripMargin)
    ) match {
      case Success(_) => Right(logger.info(s"(${properties.mkString(",")}) properties were set successfully for $catalog.$schema.$deltaObj"))
      case Failure(e) => Left(new DeltaTableManagerException(s"Error setting ${properties.mkString(",")} properties for $catalog.$schema.$deltaObj Delta table\n" +
        s"Reason: ${e.getMessage}", e))
    }
  }

  /**
   * Drop a column for a Delta Lake table using a catalog, schema, delta object and column name as arguments.
   *
   * @param catalog    The catalog containing the Delta Lake table.
   * @param schema     The schema containing the Delta Lake table.
   * @param deltaObj   The name of the Delta Lake table.
   * @param columnName The column to be dropped.
   * @return Nothing (Unit)
   * @throws Exception if an error occurs while deleting a column for Delta Lake table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def dropColumnDeltaTable(catalog: String, schema: String, deltaObj: String, columnName:String): Either[Throwable, Unit] = {
    logger.info(s"Dropping $columnName column in $catalog.$schema.$deltaObj Delta table")
    Try(SparkConfiguration.spark.sql(s"ALTER TABLE $catalog.$schema.$deltaObj DROP COLUMN `$columnName`")) match {
      case Success(_) => Right(logger.info(s"$columnName column was dropped successfully in $catalog.$schema.$deltaObj"))
      case Failure(e) => Left(new DeltaTableManagerException(s"Error deleting $columnName column in $catalog.$schema.$deltaObj Delta table\n" +
        s"Reason: ${e.getMessage}", e))
    }
  }

  /**
   * Reorganize a Delta Lake table using a catalog, schema, delta object, where condition and an apply clause as arguments.
   *
   * @param catalog     The catalog containing the Delta Lake table.
   * @param schema      The schema containing the Delta Lake table.
   * @param deltaObj    The name of the Delta Lake table.
   * @param where       The where condition.
   * @param applyClause The clause to be apply.
   * @return Nothing (Unit)
   * @throws Exception if an error occurs while deleting a column for Delta Lake table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def reorgDeltaTable(catalog: String, schema: String, deltaObj: String, where: String = "", applyClause: String = "PURGE"): Either[Throwable, Unit] = {
    logger.info(s"Reorganizing $catalog.$schema.$deltaObj Delta table, with APPLY clause: $applyClause")
    Try(SparkConfiguration.spark.sql(s"REORG TABLE $catalog.$schema.$deltaObj $where APPLY ($applyClause)")) match {
      case Success(_) => Right(logger.info(s"$catalog.$schema.$deltaObj was reorganized successfully with WHERE condition: $where and APPLY clause: $applyClause"))
      case Failure(e) => Left(new DeltaTableManagerException(s"Error reorganizing $catalog.$schema.$deltaObj Delta table with WHERE condition: $where and APPLY clause: $applyClause\n" +
        s"Reason: ${e.getMessage}", e))
    }
  }

  /**
   * Add a column for a Delta Lake table using a catalog, schema, delta object, column name and column type as arguments.
   *
   * @param catalog    The catalog containing the Delta Lake table.
   * @param schema     The schema containing the Delta Lake table.
   * @param deltaObj   The name of the Delta Lake table.
   * @param columnName The column to be added.
   * @param columntype The type for the new column.
   * @return Nothing (Unit)
   * @throws Exception if an error occurs while adding a column for Delta Lake table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def addColumnDeltaTable(catalog: String, schema: String, deltaObj: String, columnName: String, columnType: String = "STRING"): Either[Throwable, Unit] = {
    logger.info(s"Adding $columnName column ($columnType) in $catalog.$schema.$deltaObj Delta table")
    Try(SparkConfiguration.spark.sql(s"ALTER TABLE $catalog.$schema.$deltaObj ADD COLUMNS (`$columnName` $columnType)")) match {
      case Success(_) => Right(logger.info(s"$columnName column ($columnType) was added successfully in $catalog.$schema.$deltaObj Delta table"))
      case Failure(e) => Left(new DeltaTableManagerException(s"Error adding $columnName column ($columnType) in $catalog.$schema.$deltaObj Delta table\n" +
        s"Reason: ${e.getMessage}", e))
    }
  }

  /**
   * Rename a column for a Delta Lake table using a catalog, schema, delta object, column name and new column name as arguments.
   *
   * @param catalog    The catalog containing the Delta Lake table.
   * @param schema     The schema containing the Delta Lake table.
   * @param deltaObj   The name of the Delta Lake table.
   * @param columnName The column to be renamed.
   * @param newColumnName The new name for the column to be renamed.
   * @return Nothing (Unit)
   * @throws Exception if an error occurs while adding a column for Delta Lake table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def renameColumnDeltaTable(catalog: String, schema: String, deltaObj: String, columnName: String, newColumnName: String): Either[Throwable, Unit] = {
    logger.info(s"Renaming $columnName column to $newColumnName, in $catalog.$schema.$deltaObj Delta table")
    Try(SparkConfiguration.spark.sql(s"ALTER TABLE $catalog.$schema.$deltaObj RENAME COLUMN `$columnName` TO `$newColumnName`")) match {
      case Success(_) => Right(logger.info(s"$columnName column was renamed successfully to $newColumnName in $catalog.$schema.$deltaObj Delta table"))
      case Failure(e) => Left(new DeltaTableManagerException(s"Error renaming $columnName column to $newColumnName in $catalog.$schema.$deltaObj Delta table\n" +
        s"Reason: ${e.getMessage}", e))
    }
  }

  /**
   * Rename a column for a Delta Lake table using a catalog, schema, delta object, column name and column type as arguments.
   *
   * @param catalog       The catalog containing the Delta Lake table.
   * @param schema        The schema containing the Delta Lake table.
   * @param deltaObj      The name of the Delta Lake table.
   * @param columnName    The column to be typed.
   * @param columnType    The type for the column to be typed.
   * @return Nothing (Unit)
   * @throws Exception if an error occurs while typing a column for Delta Lake table.
   */
  @throws(classOf[DeltaTableManagerException])
  override def typeColumnDeltaTable(catalog: String, schema: String, deltaObj: String, columnName: String, columnType: String): Either[Throwable, Unit] = {
    val spark = SparkConfiguration.spark
    import spark.implicits._
    logger.info(s"Typing $columnName column to $columnType, in $catalog.$schema.$deltaObj Delta table")

    Try(DeltaTable.forName(s"$catalog.$schema.$deltaObj")) match {
      case Success(deltaTableResult) =>
        val partitionColumns = deltaTableResult.detail().select("partitionColumns").as[List[String]].head
        val tablePath = deltaTableResult.detail().select("location").as[String].head
        val overwriteSchemaOption = Map("overwriteSchema" -> "true")

        val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite,
          partitionBy = partitionColumns, otherOptions = overwriteSchemaOption)

        val df = deltaTableResult.toDF.withColumn(columnName, col(columnName).cast(columnType))

        Try(sparkWriteData(s"$catalog.$schema.$deltaObj", df, tablePath, "saveAsTable", sparkOptions)) match {
          case Success(_) => Right(logger.info(s"$columnName column was retyped successfully to $columnType in $catalog.$schema.$deltaObj Delta table"))
          case Failure(e) => Left(new DeltaTableManagerException(s"Error overwriting $catalog.$schema.$deltaObj Delta table to cast $columnName column to $columnType type\n" +
            s"Reason: ${e.getMessage}", e))
        }
      case Failure(exception) =>
        Left(new DeltaTableManagerException(s"Error reading the entire Delta table at $catalog.$schema.$deltaObj: ${exception.getMessage}", exception))
    }
  }
}
