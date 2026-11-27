package com.gestamp.hypatia.adls

import com.gestamp.hypatia.databricks.DatabricksManager
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.changePermissionsTeamAdmins
import com.gestamp.hypatia.exceptions.ADLS2ManagerException
import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import com.gestamp.hypatia.utils.CommonsConstants
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object ADLS2Manager extends IADLS2Manager {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override val tenantGestamp = new DatabricksManager().dbUtilsGetSecret(KeyCredentials.tenantId)
  override val clientId = new DatabricksManager().dbUtilsGetSecret(KeyCredentials.clientId)
  override val clientSecret = new DatabricksManager().dbUtilsGetSecret(KeyCredentials.clientSecret)


  /**
   * Sets the OAuth configuration for accessing Azure Data Lake Storage Gen2 using client credentials.
   *
   * @return a Configuration object containing the OAuth configuration for accessing Azure Data Lake Storage Gen2.
   */
  override def setADLS2OAuthSparkConfiguration(): Configuration = {
    logger.info(s"ADLS Gen2 OAuth Configuration with clientId =  ${KeyCredentials.clientId}, " +
      s"clientSecret = ${KeyCredentials.clientSecret} and tenantId = ${KeyCredentials.tenantId}")

    val sparkCon = SparkConfiguration.spark.sparkContext.hadoopConfiguration
    
    sparkCon.set("fs.azure.account.auth.type", "OAuth")
    sparkCon.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    sparkCon.set("fs.azure.account.oauth2.client.id", clientId)
    sparkCon.set("fs.azure.account.oauth2.client.secret", clientSecret)
    sparkCon.set("fs.azure.account.oauth2.client.endpoint", s"https://login.microsoftonline.com/$tenantGestamp/oauth2/token")
    sparkCon
  }


  /**
   * Writes the specified data frame to a table in the Azure Data Lake Storage Gen2 using the specified options.
   *
   * If the table does not exist yet it is created with the `path` option, so it is born as an EXTERNAL table in
   * that path. If the table already exists it is written by name, without the `path` option: Databricks rejects
   * any path-based access to a table with row filter or column mask (Security Mesh), and writing by name keeps its
   * tags, row filters, grants, history and LOCATION. Before writing by name, the LOCATION of the existing table must
   * match the requested path; otherwise nothing is written and an ADLS2ManagerException is returned.
   *
   * @param deltaTableName a String representing the name of the table to write the data frame
   * @param dataFrame      a DataFrame object containing the data to be written
   * @param path           a String representing the path in Azure Data Lake Storage Gen2 where the data will be written
   * @param loadType     a String representing the type of insert ("saveAsTable" or "insertInto") we want to use when writing
   * @param sparkOptions   a SparkOptions object containing the options for writing the data frame
   * @return a String representing the path in Azure Data Lake Storage Gen2 where the data was written. If the computation
   *         fails, an ADLS2ManagerException exception will be thrown indicating the error that occurred.
   */
  @throws(classOf[ADLS2ManagerException])
  override def sparkWriteData(deltaTableName: String, dataFrame: DataFrame, path: String, loadType: String = "saveAsTable", sparkOptions: SparkOptions): Either[Throwable, Unit] = {
    logger.info(s"Start build dataFrameWriter Object, datasetName = $deltaTableName, path = $path, " +
      s"Spark Options = ${sparkOptions.fileFormat} -- ${sparkOptions.saveMode}")
    // Sin setADLS2OAuthSparkConfiguration(): usa spark.sparkContext, que no existe en clusters de acceso estandar
    // (USER_ISOLATION) -> NoSuchMethodError. Esos clusters son obligatorios para tablas con ABAC (Security Mesh) y el
    // acceso al Data Lake lo resuelven las credenciales de Unity Catalog.

    resolveWriteTarget(deltaTableName, path) match {
      case Left(e) => Left(e)
      case Right(writePath) =>
        val df = dataFrame.drop("_change_type", "_commit_version", "_commit_timestamp", "_rescued_data")

        val dataFrameWriter: DataFrameWriter[Row] = df.write
          .format(sparkOptions.fileFormat)
          .mode(sparkOptions.saveMode)

        writePath match {
          case Some(p) =>
            logger.info(s"Table $deltaTableName does not exist, it is created with path = $p")
            dataFrameWriter.option("path", p)
          case None =>
            logger.info(s"Table $deltaTableName already exists in path = $path, writing by table name " +
              s"(compatible with row filter / column mask)")
        }

        if (sparkOptions.otherOptions.nonEmpty) {
          logger.info(s"DataFrameWriter Object other Options = ${sparkOptions.otherOptions.toString()}")
          dataFrameWriter.options(sparkOptions.otherOptions)
        }

        if (sparkOptions.partitionBy.nonEmpty && sparkOptions.partitionBy.exists(_.nonEmpty)) {
          logger.info(s"DataFrameWriter Object partitionBy = ${sparkOptions.partitionBy.mkString(",")}")
          dataFrameWriter.partitionBy(sparkOptions.partitionBy.toArray: _*)
        }

        Try(dataFrameWriter.saveAsTable(deltaTableName)) match {
          case Success(_) =>
            changePermissionsTeamAdmins("TABLE", s"$deltaTableName") match {
              case Right(_) => Right(logger.info(s"Spark writing has been successfully completed for $deltaTableName table"))
              case Left(e) => Left(e)
            }
          case Failure(e) => Left(new ADLS2ManagerException(s"Spark Writer => Something went wrong writing dataframe in the path = $path.\n" +
            s"Error detail: ${e.getMessage}", e))
        }
    }
  }

  /**
   * Checks in the catalog whether the table exists and, if it does, reads its LOCATION (DESCRIBE DETAIL) to decide
   * the write target with [[WriteTargetResolver.resolveWriteTarget]].
   *
   * @param deltaTableName the full name of the table (catalog.schema.table)
   * @param path           the path requested by the caller
   * @return Right(Some(path)) to write with the `path` option, Right(None) to write by table name, or
   *         Left(ADLS2ManagerException) if the catalog cannot be queried or the location does not match
   */
  private def resolveWriteTarget(deltaTableName: String, path: String): Either[ADLS2ManagerException, Option[String]] = {
    Try(SparkConfiguration.spark.catalog.tableExists(deltaTableName)) match {
      case Failure(e) => Left(new ADLS2ManagerException(s"Spark Writer => Error checking if table $deltaTableName exists.\n" +
        s"Error detail: ${e.getMessage}", e))
      case Success(false) => WriteTargetResolver.resolveWriteTarget(deltaTableName, tableExists = false, None, path)
      case Success(true) =>
        Try(SparkConfiguration.spark.sql(s"DESCRIBE DETAIL $deltaTableName").select("location").head().getString(0)) match {
          case Success(location) =>
            logger.info(s"Table $deltaTableName already exists with location = $location")
            WriteTargetResolver.resolveWriteTarget(deltaTableName, tableExists = true, Option(location), path)
          case Failure(e) => Left(new ADLS2ManagerException(s"Spark Writer => Error reading the location of table " +
            s"$deltaTableName (DESCRIBE DETAIL).\nError detail: ${e.getMessage}", e))
        }
    }
  }

  /**
   * Reads a data frame from a table in the Azure Data Lake Storage Gen2 using the specified options.
   *
   * @param path         a String representing the path in Azure Data Lake Storage Gen2 from where the data will be read
   * @param sparkOptions a SparkOptions object containing the options for reading the data frame
   * @return a DataFrame object containing the data read from Azure Data Lake Storage Gen2. If the computation fails, an
   *         ADLS2ManagerException exception will be thrown indicating the error that occurred.
   */
  @throws(classOf[ADLS2ManagerException])
  override def sparkReadData(path: String, sparkOptions: SparkOptions): DataFrame = {
    logger.info(s"Start build DataFrameReader Object, path = $path, " +
      s"Spark Options = ${sparkOptions.fileFormat}")
    // Sin setADLS2OAuthSparkConfiguration(): ver sparkWriteData (NoSuchMethodError en acceso estandar).

    val reader: DataFrameReader = SparkConfiguration.spark.read.format(sparkOptions.fileFormat)

    if (sparkOptions.otherOptions.nonEmpty) {
      logger.info(s"DataFrameReader Object other Options = ${sparkOptions.otherOptions.toString()}")
      reader.options(sparkOptions.otherOptions)
    }

    Try(reader.load(path)) match {
      case Success(df) => df
      case Failure(e) =>
        throw new ADLS2ManagerException(s"Spar Read => Error reading Dataframe in the path = $path", e)
    }
  }

  /**
   *
   * @param source  String that represents the source of the raw files.
   * @param dataset Object of type {@link Dataset} that contains the information of the dataset to read.
   * @return A {@link DataFrame} containing the data read from the raw files.
   *
   *  This method reads the raw files of a dataset and returns a DataFrame with the data.
   *  The raw files are stored in the ADLS2 storage system, and the method uses the ADLS2Manager class to perform the reading.
   *  The specific path where the raw files are stored is constructed using the ConstructorPaths class and the information of the dataset.
   *  The method also logs information about the process.
   */
  @throws(classOf[ADLS2ManagerException])
  override def readRawFilesToDF(source: String, dataset: Dataset): DataFrame = {
    logger.info(s"Read new files for Dataset: $dataset")

    val mounthPath = ConstructorPaths.buildRawPath(source, dataset.dataset)

    val sparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite)

    ADLS2Manager.sparkReadData(mounthPath, sparkOptions)
  }
}
