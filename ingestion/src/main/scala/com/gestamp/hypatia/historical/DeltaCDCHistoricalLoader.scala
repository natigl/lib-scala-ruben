package com.gestamp.hypatia.historical

import com.gestamp.hypatia.postgresql.model.Dataset
import com.gestamp.hypatia.utils.CommonsConstants
import com.gestamp.hypatia.spark.SparkOptions
import com.gestamp.hypatia.databricks.deltalake.DeltaTablesManager.createDatabase
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import com.gestamp.hypatia.exceptions.HistoricalIngestionManagerException
import org.apache.spark.sql.SaveMode
import com.gestamp.hypatia.autoloader.AutoLoader

import scala.util.{Failure, Success, Try}


/**
 * Cargador histórico para extracciones DELTA y CDC: reescribe la tabla de
 * raw_sources en modo append delegando en el stream estándar del Auto Loader,
 * que ya aplica la deduplicación propia de cada source.
 */
class DeltaCDCHistoricalLoader() extends HistoricalLoader {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Escribe el histórico DELTA/CDC en raw_sources: crea el esquema si no existe
   * y escribe todo el landing en modo append vía AutoLoader.sparkWriteStream.
   *
   * @param dataFrame   DataFrame con el histórico leído de landing
   * @param source      source del dataset
   * @param dataset     dataset a cargar (delta_column define las particiones)
   * @param path        ruta física destino en raw_sources
   * @param directories directorios de landing (no se usan: el orden lo da el propio stream)
   */
  override def writeHistoricalDfToRawSource(dataFrame: DataFrame, source: String, dataset: Dataset, path:String, directories: Seq[String]): Unit = {

    // Retorna una lista vacía en caso de que partition_by sea NULL en la BD
    val partitions = Option(dataset.delta_column).map(_.split(",").map(_.trim).toList).getOrElse(List.empty[String])

    createDatabase(s"${CommonsConstants.RAW_SOURCES}.$source") match {
      case Right(schemaName) =>
        logger.info(s"Write all historical data for table ${dataset.dataset} in 'raw_sources' layer")
        val sparkOptions: SparkOptions = SparkOptions(fileFormat = CommonsConstants.DELTA_FORMAT, saveMode = SaveMode.Append,
          partitionBy = partitions)

        Try(AutoLoader.sparkWriteStream(dataset, dataFrame, path, sparkOptions, source)) match {
          case Success(Right(counts)) => logger.info(s"The writing of historical data in 'raw_sources' layer for table ${dataset.dataset} was completed correctly.\n" +
            s"Number of read records (from Landing): ${counts._1}\n" +
            s"Number of written records (to raw_sources): ${counts._2}\n")
          case Success(Left(e)) => throw new HistoricalIngestionManagerException(
            s"Unexpected error by the writer of the historical autoloader: ${e.getMessage}\n" +
              s"Stack trace: ${e.getStackTrace.mkString("\n")}",
            e)
          case Failure(e) => throw new HistoricalIngestionManagerException(
            s"User configuration error by the writer of the historical autoloader: ${e.getMessage}\n" +
              s"Stack trace: ${e.getStackTrace.mkString("\n")}",
            e)
        }

      case Left(exception) =>
        throw new HistoricalIngestionManagerException(s"Error creating schema: ${CommonsConstants.RAW_SOURCES}.$source", exception)
    }
  }
}

