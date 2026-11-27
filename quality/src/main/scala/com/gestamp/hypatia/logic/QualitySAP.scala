package com.gestamp.hypatia.logic

import com.gestamp.hypatia.exceptions.QualityDefaultException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_date}
import org.apache.spark.sql.types._
import com.gestamp.hypatia.utils.QualityUtils.safeCol
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}
import scala.util.{Failure, Left, Right, Success, Try}

class QualitySAP extends QualityDefault {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Filtra el DataFrame quedándose con las filas cuya columna es igual al valor dado
   * (variante SAP de la regla filterEqual).
   *
   * @param df     DataFrame de entrada
   * @param column columna a comparar
   * @param value  valor a igualar
   * @return Right con el DataFrame filtrado, o Left con el error
   */
  override def filterEqualDataFrame( df: DataFrame, column: String, value: String): Either[Throwable, DataFrame] = {

    logger.info(s"starting filters equal to $value in $column for the dataframe")

    Try(df.filter(safeCol(column) === value)) match {
      case Success(dfResult) =>
        logger.info(s"dataframe filtering columns: $column with value $value")
        Right(dfResult)
      case Failure(e) => logger.error(s"Error applying filter in dataframe: $df in the columns $column with this values: $value", e)
        Left(new QualityDefaultException(s"Error applying filter in dataframe: $df in the columns $column with this values: $value", e))
    }
  }

  /**
   * Converts SAP-style encoded date columns into proper Spark `DateType` columns.
   *
   * For each specified column, the function applies the SAP date decoding logic:
   *   new_date = to_date((99999999 - column_value).cast("string"), "yyyyMMdd")
   *
   * This transformation supports multiple comma-separated column names.
   * The result is wrapped in an `Either` to handle errors safely.
   *
   * @param df Input DataFrame containing the columns to be converted.
   * @param columns Comma-separated list of column names to apply the SAP date format transformation.
   * @return Either an error (Left) or the transformed DataFrame with decoded date columns (Right).
   */
  def sapDateFormat(df: DataFrame, columns: String): Either[Throwable, DataFrame] = {

    logger.info(s"Applying dateFormat in the columns: $columns")

    Try(columns.split(",").map(_.trim).foldLeft(df) { (dfQua, field) =>
      dfQua.withColumn(field, to_date((lit(99999999) - safeCol(field).cast(IntegerType)).cast(StringType), "yyyyMMdd"))
    }) match {
      case Success(dfResult) =>
        logger.info(s"Applied dateFormat in the columns: $columns")
        Right(dfResult)
      case Failure(e) => logger.error(s"Error applying filter dateFormat in the column: $columns", e)
        Left(new QualityDefaultException(s"Error applying filter dateFormat in the column: $columns}\n" +
          s"Error detail: ${e.getMessage}", e))
    }
  }
}
