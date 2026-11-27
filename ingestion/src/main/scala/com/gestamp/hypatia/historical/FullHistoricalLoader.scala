package com.gestamp.hypatia.historical

import com.gestamp.hypatia.adls.ConstructorPaths.getPathCheckpoints
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.{changePermissionsTeamAdmins, createDatabase}
import com.gestamp.hypatia.exceptions.HistoricalIngestionManagerException
import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.spark.SparkOptions
import com.gestamp.hypatia.utils.CommonsConstants
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
 * Cargador histórico para extracciones FULL: reescribe la tabla de raw_sources
 * solo con los datos del último directorio de landing (la última foto completa).
 */
class FullHistoricalLoader() extends HistoricalLoader {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Escribe el histórico FULL en raw_sources: crea el esquema si no existe y
   * sobreescribe la tabla con los datos del último directorio de landing.
   *
   * @param dataFrame   DataFrame con el histórico leído de landing
   * @param source      source del dataset
   * @param dataset     dataset a cargar
   * @param path        ruta física destino en raw_sources
   * @param directories directorios de landing ordenados (se usa el último)
   */
  override def writeHistoricalDfToRawSource(dataFrame: DataFrame, source: String, dataset: Dataset, path: String, directories: Seq[String]): Unit = {

    createDatabase(s"${CommonsConstants.RAW_SOURCES}.$source") match {
      case Right(schemaName) =>
        logger.info(s"Write all historical data for table ${dataset.dataset} in 'raw_sources' layer")
        val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite)

        Try(sparkWriteStreamLastDirectory(dataset, dataFrame, path, sparkOptions, source, directories)) match {
          case Success(Right(counts)) => logger.info(s"The writing of historical data in 'raw_sources' layer for table ${dataset.dataset} was completed correctly.\n" +
            s"Number of read records (from Landing): ${counts._1}\n" +
            s"Number of written records (to raw_sources): ${counts._2}\n")
          case Success(Left(e)) => throw new HistoricalIngestionManagerException(
            s"Historical autoloader writer error: ${e.getMessage}\n" +
              s"Stack trace: ${e.getStackTrace.mkString("\n")}",
            e)
          case Failure(e) => throw new HistoricalIngestionManagerException(
            s"Historical autoloader writer configuration error: ${e.getMessage}\n" +
              s"Stack trace: ${e.getStackTrace.mkString("\n")}",
            e)
        }
      case Left(exception) =>
        throw new HistoricalIngestionManagerException(s"Error creating schema: ${CommonsConstants.RAW_SOURCES}.$source", exception)
    }
  }

  /**
   * Stream de escritura que filtra el batch quedándose solo con los ficheros del
   * último directorio de landing y sobreescribe la tabla destino con ellos.
   *
   * @param dataset      dataset a cargar
   * @param dataFrame    DataFrame de streaming leído de landing
   * @param path         ruta física destino
   * @param sparkOptions opciones de escritura
   * @param source       source del dataset
   * @param directories  directorios de landing ordenados
   * @return Right con (filas leídas, filas escritas), o Left con el error
   */
  private def sparkWriteStreamLastDirectory(dataset: Dataset, dataFrame: DataFrame, path: String, sparkOptions: SparkOptions, source: String, directories: Seq[String]): Either[Throwable, (Long,Long)] = {
    logger.info(s"Start stream configurations to write all historical data in the correct order to the path: $path")

    val checkpoint_path = s"${getPathCheckpoints()}/$source/${dataset.dataset}"
    var readCount = 0L
    var writeCount = 0L

    val writer: DataStreamWriter[Row] = dataFrame.selectExpr("*", "_metadata as source_metadata").writeStream
      .foreachBatch { (df: DataFrame, bacthId: Long) =>
        logger.info(s"Batch Id = $bacthId")
        logger.info(s"source = $source AND extract_type = ${dataset.extract_type}")

        readCount = df.count()
        logger.info(s"Number of rows read from Landing: $readCount")

        val lastDirectory = directories.lastOption.getOrElse {
          throw new Exception("Last load was not found in Landing layer")
        }.toString

        val dfExtraction = df
          .select("*","source_metadata.file_path")
          .filter(col("source_metadata.file_path").contains(lastDirectory))
          .drop("source_metadata","file_path")

        writeCount = dfExtraction.count()
        logger.info(s"Number of rows to be loaded from last landing directory ($lastDirectory): $writeCount")

        val dataFrameWriter: DataFrameWriter[Row] = dfExtraction.write
          .format(sparkOptions.fileFormat)
          .mode(sparkOptions.saveMode)
          .option("path", path)

        if (sparkOptions.otherOptions.nonEmpty) {
          logger.info(s"Full Historical Loader Writer Object other Options = ${sparkOptions.otherOptions.toString()}")
          dataFrameWriter.options(sparkOptions.otherOptions)
        }

        if (sparkOptions.partitionBy.nonEmpty) {
          logger.info(s"Full Historical Loader Writer Object partitionBy = ${sparkOptions.partitionBy.mkString(",")}")
          dataFrameWriter.partitionBy(sparkOptions.partitionBy.toArray: _*)
        }

        dataFrameWriter.saveAsTable(s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset}")

      }
      .trigger(Trigger.Once())
      .option("checkpointLocation", checkpoint_path)

    Try(writer.start().awaitTermination()) match {
      case Success(_) =>
        changePermissionsTeamAdmins("TABLE", s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset}") match {
          case Right(_) =>
            logger.info(
              s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset} table has been loaded by historical Autoloader\n" +
              s"Rows read: $readCount\n" +
              s"Written rows:  $writeCount\n"
            )
            Right(readCount, writeCount)
          case Left(e) => Left(e)
        }
      case Failure(exception) => Left(new HistoricalIngestionManagerException(
        s"WriteStreamLastDirectory => Error writing historical dataframe in the path $path\n" +
        s"Error detail: ${exception.getMessage}",
        exception))
    }
  }
}

