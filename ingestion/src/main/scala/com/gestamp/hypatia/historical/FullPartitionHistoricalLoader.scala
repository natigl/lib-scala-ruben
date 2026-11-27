package com.gestamp.hypatia.historical

import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.utils.CommonsConstants
import com.gestamp.hypatia.spark.SparkOptions
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.createDatabase
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import com.gestamp.hypatia.adls.ConstructorPaths.getPathCheckpoints
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import org.slf4j.{Logger, LoggerFactory}
import com.gestamp.hypatia.exceptions.HistoricalIngestionManagerException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.changePermissionsTeamAdmins
import scala.util.{Failure, Success, Try}

/**
 * Cargador histórico para extracciones FULL_PARTITION: reescribe la tabla de
 * raw_sources procesando los directorios de landing en orden, con sobreescritura
 * dinámica de particiones.
 */
class FullPartitionHistoricalLoader() extends HistoricalLoader {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Escribe el histórico particionado en raw_sources: crea el esquema si no existe
   * y sobreescribe partición a partición en el orden de los directorios de landing.
   *
   * @param dataFrame   DataFrame con el histórico leído de landing
   * @param source      source del dataset
   * @param dataset     dataset a cargar (delta_column define las particiones)
   * @param path        ruta física destino en raw_sources
   * @param directories directorios de landing a procesar, ordenados
   */
  override def writeHistoricalDfToRawSource(dataFrame: DataFrame, source: String, dataset: Dataset, path: String, directories: Seq[String]): Unit = {

    // Retorna una lista vacía en caso de que partition_by sea NULL en la BD
    val partitions = Option(dataset.delta_column).map(_.split(",").map(_.trim).toList).getOrElse(List.empty[String])

    createDatabase(s"${CommonsConstants.RAW_SOURCES}.$source") match {
      case Right(schemaName) =>
        logger.info(s"Write all historical data for table ${dataset.dataset} in 'raw_sources' layer")
        val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Overwrite,
          partitionBy = partitions, otherOptions = Map("partitionOverwriteMode" -> "dynamic"))

        Try(sparkWriteStreamPerDirectory(dataset, dataFrame, path, sparkOptions, source, directories)) match {
          case Success(Right(read_count)) => logger.info(s"The writing of historical data in 'raw_sources' layer for table ${dataset.dataset} was completed correctly.\n" +
            s"Number of read records (from Landing): $read_count")
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
        throw new HistoricalIngestionManagerException(s"Error creating schema: ${CommonsConstants.RAW_CLEAN}.$source.${dataset.dataset}", exception)
    }
  }

  /**
   * Stream de escritura que, dentro de cada batch, filtra y escribe los datos
   * directorio a directorio (en orden) para respetar la secuencia temporal de
   * las extracciones al sobreescribir particiones.
   *
   * @param dataset      dataset a cargar
   * @param dataFrame    DataFrame de streaming leído de landing
   * @param path         ruta física destino
   * @param sparkOptions opciones de escritura
   * @param source       source del dataset
   * @param directories  directorios de landing ordenados
   * @return Right con las filas leídas, o Left con el error
   */
  private def sparkWriteStreamPerDirectory(dataset: Dataset, dataFrame: DataFrame, path: String, sparkOptions: SparkOptions, source: String, directories: Seq[String]): Either[Throwable, Long] = {
    logger.info(s"Start stream configurations to write all historical data in the correct order to the path: $path")

    val checkpoint_path = s"${getPathCheckpoints()}/$source/${dataset.dataset}"
    var readCount = 0L

    val writer: DataStreamWriter[Row] = dataFrame.selectExpr("*", "_metadata as source_metadata").writeStream
      .foreachBatch { (df: DataFrame, bacthId: Long) =>
        logger.info(s"Batch Id = $bacthId")
        logger.info(s"source = $source AND extract_type = ${dataset.extract_type}")

        readCount = df.count()
        logger.info(s"Number of rows read from Landing: $readCount")

        directories.foreach(dir => {
          val dfExtraction = df
            .select("*","source_metadata.file_path")
            .filter(col("source_metadata.file_path").contains(dir.toString))
            .drop("source_metadata","file_path")

          logger.info(s"Number of rows to be loaded from $dir landing directory: ${dfExtraction.count()}")

          val dataFrameWriter: DataFrameWriter[Row] = dfExtraction.write
            .format(sparkOptions.fileFormat)
            .mode(sparkOptions.saveMode)
            .option("path", path)

          if (sparkOptions.otherOptions.nonEmpty) {
            logger.info(s"Full Partition Historical Loader Writer Object other Options = ${sparkOptions.otherOptions.toString()}")
            dataFrameWriter.options(sparkOptions.otherOptions)
          }

          if (sparkOptions.partitionBy.nonEmpty) {
            logger.info(s"Full Partition Historical Loader Writer Object partitionBy = ${sparkOptions.partitionBy.mkString(",")}")
            dataFrameWriter.partitionBy(sparkOptions.partitionBy.toArray: _*)
          }

          dataFrameWriter.saveAsTable(s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset}")
        }
        )
      }
      .trigger(Trigger.Once())
      .option("checkpointLocation", checkpoint_path)

    Try(writer.start().awaitTermination()) match {
      case Success(res) =>
        changePermissionsTeamAdmins("TABLE", s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset}") match {
          case Right(_) =>
            logger.info(
              s"${CommonsConstants.RAW_SOURCES}.$source.${dataset.dataset} table has been loaded by historical Autoloader\n" +
              s"Rows read: $readCount"
            )
            Right(readCount)
          case Left(e) => Left(e)
        }
      case Failure(exception) => Left(new HistoricalIngestionManagerException(
        s"WriteStreamPerDirectory => Error writing historical dataframe in the path $path\n" +
        s"Error detail: ${exception.getMessage}",
        exception))
    }
  }
}

