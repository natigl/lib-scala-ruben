package com.gestamp.hypatia.logic

import com.gestamp.hypatia.utils.QualityUtils.safeCol
import com.gestamp.hypatia.exceptions.QualityDefaultException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Left, Right, Success, Try}
import io.circe.parser.parse

class QualityJSON {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Parses string columns containing JSON content and converts them into complex types
   * (`struct` or `array`) within the given DataFrame.
   *
   * For each string column, the function tries to parse its first value as JSON and, if valid,
   * infers the schema and replaces the column using `from_json`. Non-JSON columns remain unchanged.
   *
   * Returns the transformed DataFrame wrapped in an `Either` for safe error handling.
   *
   * @param df Input DataFrame.
   * @return Either an error (Left) or the transformed DataFrame (Right).
   */
  def jsonToComplexType(df: DataFrame): Either[Throwable, DataFrame] = {
    logger.info(s"Processing json columns")

    Try(
      df.schema.fields.foldLeft(df) { (tempDF, f) =>
        f.dataType match {
          case _: StringType => {
            val columnValue = df.select(safeCol(f.name)).first().getString(0)
            parse(columnValue) match {
              case Right(json) =>
                if (json.isArray || json.isObject)
                  tempDF.withColumn(f.name, from_json(safeCol(f.name), schema_of_json(lit(columnValue))))
                else tempDF
              case Left(error) => tempDF
            }
          }
          case _ => tempDF
        }
      }) match {
        case Success(finalDF) =>
          logger.info(s"jsonToComplexType quality rule transformation completed")
          Right(finalDF)
        case Failure(e) => logger.error(s"Cannot Parse json columns", e)
          Left(new QualityDefaultException(s"Cannot Parse json columns}\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    }
}
