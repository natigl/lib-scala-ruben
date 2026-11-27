package com.gestamp.hypatia.autoloader

import com.gestamp.hypatia.adls.ADLS2Manager
import com.gestamp.hypatia.adls.ConstructorPaths.getPathCheckpoints
import com.gestamp.hypatia.databricks.deltalake.DeltaChangeDataFeed.upsertDeltaTableChangeDataCapture
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.changePermissionsTeamAdmins
import com.gestamp.hypatia.exceptions.AutoLoaderException
import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import com.gestamp.hypatia.utils.CommonsConstants
import com.gestamp.hypatia.utils.IngestionUtils.castInvalidChars
import io.delta.tables.DeltaTable
import com.gestamp.hypatia.utils.TriggerUtils
import java.net.URLDecoder._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, size, split}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}

object AutoLoader extends IAutoLoaderBuilder {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override val configsDataLake = ADLS2Manager.setADLS2OAuthSparkConfiguration()


  /**
   * This method implements the Auto Loader reader stream for reading the last files ingested in Staging layer
   * by each dataset.
   *  - It's important check the file format because, each file format has a different Auto Loader configuration
   *
   * @param dataset
   * @param path
   * @return
   */
  @throws(classOf[AutoLoaderException])
  override def sparkReadStream(autoloaderConfig: Map[String, String]): Either[Throwable, DataFrame] = {
    logger.info(s"Start configurations stream to begin reading incoming files " +
      s"from the ${autoloaderConfig.get("path")}")

    val reader: DataStreamReader = SparkConfiguration.spark.readStream
      .format("cloudFiles")
      .options(autoloaderConfig)

    Try(reader.load()) match {
      case Success(df) => Right(df)
      case Failure(exception) =>
        Left(new AutoLoaderException(
          s"Autoloader StreamReader => Error reading dataframe from the path = ${autoloaderConfig.get("path")}\n" +
          s"Error detail: ${exception.getMessage}",
          exception))
    }
  }

  /**
   * This method implements the Auto Loader writer stream for writing the last files ingested in Staging layer
   * by each dataset.
   * @param dataFrame
   * @param mountPath
   * @param sparkOptions
   * @return
   */
  @throws(classOf[AutoLoaderException])
  override def sparkWriteStream(dataset: Dataset, dataFrame: DataFrame, path: String, sparkOptions: SparkOptions, source: String): Either[Throwable, (Long,Long)] = {
    logger.info(s"Start stream configuration to start writing incoming files from the $path")

    val checkpoint_path = s"${getPathCheckpoints()}/$source/${dataset.dataset}"
    var readCount = 0L
    var writeCount = 0L

    val writer: DataStreamWriter[Row] = dataFrame.selectExpr("*", "_metadata as source_metadata").writeStream
      .foreachBatch { (df: DataFrame, batchId: Long) =>
        logger.info(s"Batch Id = $batchId")
        logger.info(s"source = $source AND extract_type = ${dataset.extract_type}")
        if (dataset.extract_type != CommonsConstants.STREAM_EXTRACTION) logDistinctDirectories(df, source)

        readCount = df.count()
        logger.info(s"Number of rows read from Landing: $readCount")


        val editedDF = (source, dataset.extract_type, dataset.format) match {
          case (CommonsConstants.SAP_ECC, CommonsConstants.CDC, "parquet") =>
            df.filter(col("GLDELFLAG") =!= "D").drop("GLREQUEST", "GLDELFLAG", "GLCHANGETIME", "GLCDCFLAG","source_metadata")
          case (CommonsConstants.SAP_ECC, CommonsConstants.CDC, _) =>
            getRowsMaxSequenceNumber(df, dataset.key_columns).filter(col("ODQ_CHANGEMODE") =!= "D").drop("ODQ_CHANGEMODE", "ODQ_ENTITYCNTR", "_PACKAGEID", "_SEQUENCENUMBER","source_metadata")
          case (CommonsConstants.SAP_SNP, CommonsConstants.CDC, _) =>
            df.filter(col("GLDELFLAG") =!= "D").drop("GLREQUEST", "GLDELFLAG", "GLCHANGETIME", "GLCDCFLAG","source_metadata")
          case (CommonsConstants.SAP_ARIBA, CommonsConstants.CDC, _) =>
            getRowsMaxTimeSequence(df, dataset.key_columns).drop("source_metadata")
          case (_, CommonsConstants.DELTA, _) =>
            df.dropDuplicates().drop("source_metadata")
          case _ =>
            df.drop("source_metadata")
        }

        writeCount = editedDF.count()
        logger.info(s"Number of rows to be loaded in raw_sources: $writeCount")

        val dataFrameWriter: DataFrameWriter[Row] = editedDF.write
          .format(sparkOptions.fileFormat)
          .mode(sparkOptions.saveMode)
          .option("path", path)

        if (sparkOptions.otherOptions.nonEmpty) {
          logger.info(s"Auto Loader DataFrameWriter Object other Options = ${sparkOptions.otherOptions.toString()}")
          dataFrameWriter.options(sparkOptions.otherOptions)
        }

        if (sparkOptions.partitionBy.nonEmpty && sparkOptions.partitionBy.exists(_.nonEmpty)) {
          logger.info(s"Auto Loader DataFrameWriter Object partitionBy = ${sparkOptions.partitionBy.mkString(",")}")
          dataFrameWriter.partitionBy(sparkOptions.partitionBy.toArray:_*)
        }

        dataFrameWriter.saveAsTable(s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset}")
      }
      .trigger(TriggerUtils.getTrigger(dataset.extract_type))
      .option("checkpointLocation", checkpoint_path)

    Try(writer.start().awaitTermination()) match {
      case Success(_) =>
        changePermissionsTeamAdmins("TABLE", s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset}") match {
          case Right(_) =>
            logger.info(
              s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset} table has been loaded by Autoloader\n" +
              s"Rows read: $readCount\n" +
              s"Written rows:  $writeCount\n"
            )
            Right(readCount, writeCount)
          case Left(e) => Left(e)
        }
      case Failure(exception) => Left(new AutoLoaderException(
        s"Autoloader StreamWriter => Error writing dataframe in the path = $path\n" +
        s"Error detail: ${exception.getMessage}",
        exception))
    }
  }

  /**
   * Inicia un flujo de escritura de Spark para fusionar los datos de Delta en una tabla Delta existente.
   *
   * @throws AutoLoaderException si se produce un error al escribir el DataFrame en la ruta especificada.
   * @param dataset El conjunto de datos de la sesión cuyos datos se van a fusionar.
   * @param dataFrame El DataFrame con los datos del conjunto de datos que se van a fusionar.
   * @param path La ruta de la tabla Delta donde se fusionarán los datos.
   * @param source El nombre de la fuente de los datos que se van a fusionar.
   * @return Un objeto Right con el nombre de la tabla Delta en la que se fusionaron los datos, o un objeto Left si hubo una excepción.
   */
  @throws(classOf[AutoLoaderException])
  override def sparkWriteStreamMerge(dataset: Dataset, dataFrame: DataFrame, path: String, source: String): Either[Throwable, (Long,Long)] = {
    logger.info(s"Start stream configuration to start writing incoming files from the $path")

    val checkpoint_path = s"${getPathCheckpoints()}/$source/${dataset.dataset}"
    var writeCount = 0L
    var readCount = 0L

    val writer: DataStreamWriter[Row] = dataFrame.selectExpr("*", "_metadata as source_metadata").writeStream
      .foreachBatch{ (df: DataFrame, bacthId: Long) =>
        logger.info(s"Batch Id: $bacthId")
        logger.info(s"source = $source AND extract_type = ${dataset.extract_type}")
        if (dataset.extract_type != CommonsConstants.STREAM_EXTRACTION) logDistinctDirectories(df, source)

        val targetDeltaTable = DeltaTable.forPath(SparkConfiguration.spark, path)

        readCount = df.count()
        logger.info(s"Number of rows read from Landing: $readCount")

        val editedDF = (source, dataset.extract_type) match {
          case (CommonsConstants.SAP_ECC, CommonsConstants.CDC) =>
            if(isSnp(dataset))
              df.drop("source_metadata")
            else
              getRowsMaxSequenceNumber(df, dataset.key_columns)
          case (CommonsConstants.SAP_ARIBA, CommonsConstants.CDC) =>
            getRowsMaxTimeSequence(df, dataset.key_columns)
          case _ =>
            df.drop("source_metadata")
        }

        writeCount = editedDF.count()
        logger.info(s"Number of rows to upsert: $writeCount")

        upsertDeltaTableChangeDataCapture(targetDeltaTable, editedDF, dataset.key_columns.split(",")) match {
          case Right(_) => logger.info(s"Finished merge delta table in Raw Source layer in $path")
          case Left(exception) =>
            throw new AutoLoaderException(s"Error merging delta table in raw_sources layer in $path path\n${exception.getMessage}",
              exception)
        }
      }
      .trigger(Trigger.Once())
      .option("checkpointLocation", checkpoint_path)

    Try(writer.start().awaitTermination()) match {
      case Success(_) =>
        changePermissionsTeamAdmins("TABLE", s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset}") match {
          case Right(_) =>
            logger.info(s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset} table has been loaded by Autoloader")
            Right(readCount,writeCount)
          case Left(e) => Left(e)
        }
      case Failure(exception) => Left(new AutoLoaderException(
        s"Autoloader StreamWriter => Error writing dataframe in the path = $path\n" +
        s"Error detail: ${exception.getMessage}",
        exception))
    }
  }

  private def isSnp(dataset: Dataset): Boolean = {
    dataset.group.contains(CommonsConstants.SNP)
  }


  /**
   * Dado un DataFrame y para una combinación de columnas clave, obtiene las filas que tienen el número de secuencia máximo..
   *
   * @param dataframe El DataFrame en el que se buscará el número de secuencia máximo.
   * @param keyColumns Las columnas clave que se utilizarán para buscar el número de secuencia máximo.
   * @return El DataFrame con las filas que tienen el número de secuencia máximo para cada combinación de columnas clave.
   */
  private def getRowsMaxSequenceNumber(dataframe: DataFrame, keyColumns: String): DataFrame = {
    logger.info(s"Get Rows with Max Sequence Number for keycolumns: $keyColumns")

    val seqKeColumns = keyColumns.split(",").toSeq
    val seqKeColumnsWithDirectory = seqKeColumns ++ Seq("directory")

    val dfColumnsCasted = castInvalidChars(dataframe)

    val windowSpec1 = Window.partitionBy(seqKeColumnsWithDirectory.map(col): _*).orderBy(col("_PACKAGEID").desc, col("_SEQUENCENUMBER").desc)
    val windowSpec2 = Window.partitionBy(seqKeColumns.map(col): _*).orderBy(col("directory").desc)

    dfColumnsCasted
      .select("*", "source_metadata.file_path", "source_metadata.file_modification_time")
      .withColumn("directory", split(col("file_path"), "/")(size(split(col("file_path"), "/")) - 3).cast("long"))
      .withColumn("_PACKAGEID", col("_PACKAGEID").cast("Integer"))
      .withColumn("_SEQUENCENUMBER", col("_SEQUENCENUMBER").cast("Integer"))
      .withColumn("time_sequence_per_directory", row_number().over(windowSpec1))
      .filter(col("time_sequence_per_directory") === 1)
      .withColumn("time_sequence", row_number().over(windowSpec2))
      .filter(col("time_sequence") === 1)
      .drop("time_sequence_per_directory", "time_sequence", "file_path", "file_modification_time", "directory")
  }

  private def getRowsMaxTimeSequence(dataframe: DataFrame, keyColumns: String): DataFrame = {
    logger.info(s"Get Rows with Max Time Sequence for keycolumns: $keyColumns")

    val seqKeColumns = keyColumns.split(",").toSeq
    val dfColumnsCasted = castInvalidChars(dataframe)
    val windowSpec = Window.partitionBy(seqKeColumns.map(col): _*).orderBy(col("directory").desc)

    dfColumnsCasted
      .select("*","source_metadata.file_path")
      .withColumn("directory", split(col("file_path"), "/")(size(split(col("file_path"), "/")) - 2).cast("long"))
      .withColumn("time_sequence", row_number().over(windowSpec))
      .filter(col("time_sequence") === 1)
      .drop("time_sequence", "file_path", "directory")
  }

  private def logDistinctDirectories(df: DataFrame, source: String): Unit = {
    val level = if (source == CommonsConstants.SAP_ECC) 3 else 2
    val distinctDirectories = df
      .withColumn("directory", split(col("source_metadata.file_path"), "/")(size(split(col("source_metadata.file_path"), "/")) - level))
      .select("directory")
      .distinct()
      .collect()
      .map(_.get(0).toString)
    logger.info(s"Distinct directories detected to be processed by Autoloader: ${distinctDirectories.mkString(", ")}")
  }

}
