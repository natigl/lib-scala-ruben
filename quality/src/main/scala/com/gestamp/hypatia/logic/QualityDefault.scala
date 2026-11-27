package com.gestamp.hypatia.logic

import com.gestamp.hypatia.exceptions.QualityDefaultException
import com.gestamp.hypatia.utils.ConstantsQuality
import com.gestamp.hypatia.utils.QualityUtils.{applyJsonSchemaToStringColumn, getJsonSchema, safeCol}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Left, Right, Success, Try}

class QualityDefault {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * This method drops columns of a DataFrame `df` based on the input `columns` parameter (list of column names separated by commas).
   * It returns an Either-Object with the new DataFrame in case of success, or an Exception in case of failure.
   *
   * @param df      The DataFrame in which columns should be dropped.
   * @param columns A string containing column names separated by ","
   * @return Either-Object with the new DataFrame in case of success, or an Exception in case of failure.
   *         It raises an exception if a specified column in the parameter `columns` was not found in the DataFrame.
   * @throws QualityDefaultException   It's raised when it's not possible to drop columns in the DataFrame.
   */
  def dropColumns(df: DataFrame, columns: String): Either[Throwable, DataFrame] = {
    logger.info(s"Dropping columns: $columns of DataFrame")

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      Try{
        df.drop(colArray: _*)
      } match {
        case Success(dfResult) =>
          logger.info(s"Columns dropped successfully: $columns")
          Right(dfResult)
        case Failure(e) => logger.error(s"Error dropping columns: $columns in the dataframe", e)
          Left(new QualityDefaultException(s"Error dropping columns: $columns in the dataframe}\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    } else {
      logger.error("A specified column in the parameter 'columns' was not found in the DataFrame")
      Left(new QualityDefaultException("A specified column in the parameter 'columns' was not found in the DataFrame"))
    }
  }



  /**
   * Drops the specified columns from the given DataFrame.
   *
   * @param df The DataFrame from which to drop the columns.
   * @param columns A comma-separated string of the names of the columns to be dropped. (columns = "field1,field2,field3,....")
   *
   * @return Either a new DataFrame with the specified columns dropped as a Right, or a QualityDefaultException
   * wrapped in Left if one of the specified columns does not exist in the input DataFrame.
   * @throws Throwable if any error happens during the spark or scala runtime
   */
  def filterEqualDataFrame(df: DataFrame, columns: String, value: String): Either[Throwable, DataFrame] = {
    logger.info(s"Starting dataframe filtering: the values of $columns columns must be into $value values")

    val colArray = columns.split(",").map(_.trim)
    val valueArray = value.split(",").map(_.trim)
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      Try{
        val colFilter = colArray.map(safeCol(_).isin(valueArray: _*)).reduce(_ || _)
        df.filter(colFilter)
      } match {
        case Success(dfResult) =>
          logger.info(s"Dataframe columns filtering was successfully completed: $columns columns with values into ($value)")
          Right(dfResult)
        case Failure(e) => logger.error(s"Error when applying filter in dataframe for $columns columns with ($value) values", e)
          Left(new QualityDefaultException(s"Error when applying filter in dataframe for $columns columns with ($value) values\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    } else {
      logger.error("A specified column in the parameter 'columns' was not found in the DataFrame")
      Left(new QualityDefaultException("A specified column in the parameter 'columns' was not found in the DataFrame"))
    }
  }

  /**
   * This method filters the DataFrame `df` on columns specified by `columns` where their values are not equal to `value`.
   * It returns an Either-Object with the filtered DataFrame in case of success, or an Exception in case of failure.
   *
   * @param df      DataFrame to filter.
   * @param columns String with names of columns separated by "," to filter.
   * @param value   Value to filter on column(s).
   * @return Either-Object with the filtered DataFrame in case of success, or an Exception in case of failure.
   *         It returns an Exception if a specified column in the parameter `columns` was not found in the DataFrame.
   * @throws QualityDefaultException   It is raised when it is not possible to apply the filter to the DataFrame.
   */
  def filterNotEqualDataFrame(df: DataFrame, columns: String, value: String): Either[Throwable, DataFrame] = {
    logger.info(s"Starting filter NotEqual in the columns: $columns with the value: $value for the dataframe")

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      Try{
        val colFilter = colArray.map( x => not( safeCol(x).eqNullSafe(value)) ).reduce(_ && _)
        df.filter(colFilter)
      } match {
        case Success(dfResult) =>
          logger.info(s"Dataframe filtering columns: $columns with value $value")
          Right(dfResult)
        case Failure(e) => logger.error(s"Error applying filter in dataframe: in the columns $columns with this values: $value", e)
          Left(new QualityDefaultException(s"Error applying filter in dataframe: in the columns $columns with this values: $value}\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    } else {
      logger.error("A specified column in the parameter 'columns' was not found in the DataFrame")
      Left(new QualityDefaultException("A specified column in the parameter 'columns' was not found in the DataFrame"))
    }
  }

  /**
   * This method filters the DataFrame `df` on columns specified by `columns` where their values match a regex condition.
   * It returns an Either-Object with the filtered DataFrame in case of success, or an Exception in case of failure.
   *
   * @param df      DataFrame to filter.
   * @param columns String with names of columns separated by "," to filter.
   * @param filter_pattern   Regex expression to filter on column(s).
   * @return Either-Object with the filtered DataFrame in case of success, or an Exception in case of failure.
   *         It returns an Exception if a specified column in the parameter `columns` was not found in the DataFrame.
   * @throws QualityDefaultException It is raised when it is not possible to apply the filter to the DataFrame.
   */
  def filterRegexDataFrame(df: DataFrame, columns: String, filter_pattern: String): Either[Throwable, DataFrame] = {
    logger.info(s"Starting filtering where values of $columns match the filter_pattern $filter_pattern")

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      Try {
        val colFilter = colArray.map(safeCol(_).rlike(filter_pattern)).reduce(_ || _)
        df.filter(colFilter)
      } match {
        case Success(dfResult) =>
          logger.info(s"Dataframe filtering columns: $columns. Regex filter pattern: $filter_pattern")
          Right(dfResult)
        case Failure(e) => logger.error(s"Error applying regex filter to the dataframe columns $columns, with this regex filter pattern: $filter_pattern", e)
          Left(new QualityDefaultException(s"Error applying regex filter to the dataframe columns $columns, with this regex filter pattern: $filter_pattern}\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    } else {
      logger.error("A specified column in the parameter 'columns' was not found in the DataFrame")
      Left(new QualityDefaultException("A specified column in the parameter 'columns' was not found in the DataFrame"))
    }
  }

  /**
   * Parses the `value` parameter of the FILTER_RANGE rule ("min,max") into its two optional bounds.
   * Both bounds are optional, so open ranges are allowed ("100,5000", "100," or ",5000"), but at least
   * one of them must be informed: an empty range would keep every row and it is treated as a configuration error.
   *
   * @param value String with the two comma separated bounds of the inclusive range: "min,max".
   * @return Either-Object with the (minimum, maximum) bounds in case of success, or an Exception in case of failure.
   *         It returns an Exception if the value has not exactly two bounds, if both bounds are empty,
   *         if a bound is not a finite number or if the minimum bound is greater than the maximum one.
   */
  private def parseRangeValue(value: String): Either[Throwable, (Option[Double], Option[Double])] = {
    val rangeParts = Option(value).getOrElse("").split(",", -1).map(_.trim)

    if (rangeParts.length != 2) {
      Left(new QualityDefaultException(s"Invalid value ($value) for the ${ConstantsQuality.FILTER_RANGE_PARAM} rule: " +
        s"it must contain exactly two comma separated bounds with the format 'min,max' (for example '100,5000', '100,' or ',5000')"))
    } else if (rangeParts(0).isEmpty && rangeParts(1).isEmpty) {
      Left(new QualityDefaultException(s"Invalid value ($value) for the ${ConstantsQuality.FILTER_RANGE_PARAM} rule: " +
        s"at least one of the bounds of the range 'min,max' must be informed"))
    } else {
      for {
        minValue <- parseRangeBound(rangeParts(0), "minimum", value).right
        maxValue <- parseRangeBound(rangeParts(1), "maximum", value).right
        range <- checkRangeBounds(minValue, maxValue, value).right
      } yield range
    }
  }

  /**
   * Parses one of the bounds of the FILTER_RANGE rule. An empty bound means that the range is open on that side.
   *
   * Only finite bounds are accepted: "NaN", "Infinity" and "-Infinity" are valid literals for `toDouble`, but they
   * do not describe a usable range (a NaN bound never keeps a row, so the rule would empty the dataset without
   * failing), so they are rejected as a configuration error instead of being parsed.
   *
   * @param bound     String with the bound to parse, already trimmed. An empty string means "no bound".
   * @param boundName Name of the bound ("minimum" / "maximum"), only used to build the error message.
   * @param value     Complete value of the rule, only used to build the error message.
   * @return Either-Object with the parsed bound (None when it is open) in case of success, or an Exception in case of failure.
   */
  private def parseRangeBound(bound: String, boundName: String, value: String): Either[Throwable, Option[Double]] = {
    if (bound.isEmpty) {
      Right(None)
    } else {
      Try(bound.toDouble) match {
        case Success(parsedBound) if !java.lang.Double.isFinite(parsedBound) =>
          Left(new QualityDefaultException(s"Invalid value ($value) for the ${ConstantsQuality.FILTER_RANGE_PARAM} rule: " +
            s"the $boundName bound '$bound' is not a finite number: NaN and infinite bounds are not allowed because " +
            s"they would discard every row without failing"))
        case Success(parsedBound) => Right(Some(parsedBound))
        case Failure(e) =>
          Left(new QualityDefaultException(s"Invalid value ($value) for the ${ConstantsQuality.FILTER_RANGE_PARAM} rule: " +
            s"the $boundName bound '$bound' is not a number\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    }
  }

  /**
   * Checks that the bounds of the FILTER_RANGE rule define a valid range, that is, that the minimum bound is not
   * greater than the maximum one. An inverted range never keeps a row, so it is treated as a configuration error.
   *
   * @param minValue Minimum bound of the range, None when the range is open on that side.
   * @param maxValue Maximum bound of the range, None when the range is open on that side.
   * @param value    Complete value of the rule, only used to build the error message.
   * @return Either-Object with the (minimum, maximum) bounds in case of success, or an Exception in case of failure.
   */
  private def checkRangeBounds(minValue: Option[Double], maxValue: Option[Double], value: String): Either[Throwable, (Option[Double], Option[Double])] = {
    (minValue, maxValue) match {
      case (Some(minBound), Some(maxBound)) if minBound > maxBound =>
        Left(new QualityDefaultException(s"Invalid value ($value) for the ${ConstantsQuality.FILTER_RANGE_PARAM} rule: " +
          s"the minimum bound ($minBound) is greater than the maximum bound ($maxBound)"))
      case _ => Right((minValue, maxValue))
    }
  }

  /**
   * Builds the condition that a row must satisfy to be kept by the FILTER_RANGE rule, when the range is still the
   * raw `value` parameter of the rule ("min,max").
   *
   * It never throws: when `value` is not a valid range (see parseRangeValue) or when `columns` does not contain any
   * column name (see buildRangeKeepCondition) it logs the error and returns None, so a caller that only needs the
   * condition (for instance to compute the discarded rows) can degrade gracefully instead of aborting the quality
   * process.
   *
   * @param columns String with names of columns separated by "," the rule applies to.
   * @param value   String with the two comma separated bounds of the inclusive range: "min,max".
   * @return Option with the condition that a row must satisfy to be kept, None when `value` is not a valid range
   *         or when `columns` does not contain any column name.
   */
  private def rangeKeepCondition(columns: String, value: String): Option[Column] = {
    parseRangeValue(value) match {
      case Right((minValue, maxValue)) => buildRangeKeepCondition(columns, minValue, maxValue)
      case Left(e) =>
        logger.error(s"Error building the ${ConstantsQuality.FILTER_RANGE_PARAM} condition for the columns $columns: ${e.getMessage}")
        None
    }
  }

  /**
   * Builds the condition that a row must satisfy to be kept by the FILTER_RANGE rule from the already parsed bounds:
   * every one of the given columns, casted to DoubleType, must be a finite number inside the inclusive range.
   *
   * Rows with a null, NaN or infinite value in any of the checked columns are discarded: a value that cannot be
   * casted to DoubleType becomes null, but a NaN written by the source (for instance a CSV generated with numpy)
   * stays NaN and, with the Spark comparison semantics, NaN is greater than any other value, so it would survive an
   * open range like "100," if it was not excluded explicitly (the same happens with -Infinity and a range ",5000").
   *
   * It never throws: when `columns` does not contain any column name (it is null or empty, or every one of its
   * parts is empty like in "," or ",country") it logs the error and returns None, instead of building a condition
   * over a column named "" that would fail later with an unintelligible analysis error.
   *
   * @param columns  String with names of columns separated by "," the rule applies to.
   * @param minValue Minimum bound of the range, None when the range is open on that side.
   * @param maxValue Maximum bound of the range, None when the range is open on that side.
   * @return Option with the condition that a row must satisfy to be kept, None when `columns` does not contain any column name.
   */
  private def buildRangeKeepCondition(columns: String, minValue: Option[Double], maxValue: Option[Double]): Option[Column] = {
    val colArray = Option(columns).getOrElse("").split(",").map(_.trim).filter(_.nonEmpty)

    if (colArray.isEmpty) {
      logger.error(s"Error building the ${ConstantsQuality.FILTER_RANGE_PARAM} condition: the parameter 'columns' " +
        s"($columns) must contain at least one column name")
      None
    } else {
      if (colArray.length > 1) {
        logger.warn(s"The ${ConstantsQuality.FILTER_RANGE_PARAM} rule applies the same range to every column of " +
          s"'$columns' with AND semantics: a row is kept only when all of them are inside the range. " +
          s"To check different ranges on different columns, configure one rule per range.")
      }

      Some(colArray.map { columnName =>
        val numericColumn = safeCol(columnName).cast(DoubleType)
        val finiteCondition = numericColumn.isNotNull && !isnan(numericColumn) &&
          numericColumn > lit(Double.NegativeInfinity) && numericColumn < lit(Double.PositiveInfinity)
        val minCondition = minValue.map(minBound => numericColumn >= lit(minBound)).getOrElse(lit(true))
        val maxCondition = maxValue.map(maxBound => numericColumn <= lit(maxBound)).getOrElse(lit(true))
        finiteCondition && minCondition && maxCondition
      }.reduce(_ && _))
    }
  }

  /**
   * This method filters the DataFrame `df` keeping the rows whose columns specified by `columns` are inside the
   * inclusive range described by `value` ("min,max"). Both bounds are optional, so open ranges are allowed:
   * "100,5000" (both bounds), "100," (only minimum) or ",5000" (only maximum). Having both bounds empty is a
   * configuration error, not a rule that keeps every row, so in that case it returns an Exception. The bounds must
   * also be finite numbers: "NaN,5000" or "Infinity,", which would discard every row, are configuration errors too.
   *
   * Unlike `filterEqualDataFrame`, which keeps a row when ANY of the columns matches, this rule keeps a row only
   * when ALL of the given columns are inside the range (AND semantics), because it is a quality check.
   *
   * The columns are casted to DoubleType before comparing them, so columns read as string (for instance from a
   * CSV) are supported. A value that cannot be casted becomes null when casting, and a row with a null, NaN or
   * infinite value in any of the checked columns does not satisfy the condition and is therefore discarded.
   *
   * Note that the cast to DoubleType loses precision above 2^53: it is harmless for magnitudes like file sizes or
   * measures, but the rule should not be applied to identifiers, epochs in nanoseconds or wide decimal(38,x) columns.
   *
   * @param df      DataFrame to filter.
   * @param columns String with names of columns separated by "," to filter.
   * @param value   String with the two comma separated bounds of the inclusive range: "min,max".
   * @return Either-Object with the filtered DataFrame in case of success, or an Exception in case of failure.
   *         It returns an Exception if a specified column in the parameter `columns` was not found in the DataFrame,
   *         if the parameter `columns` does not contain any column name or if the parameter `value` is not a valid range.
   * @throws QualityDefaultException It is raised when it is not possible to apply the filter to the DataFrame.
   */
  def filterRangeDataFrame(df: DataFrame, columns: String, value: String): Either[Throwable, DataFrame] = {
    logger.info(s"Starting filter Range in the columns: $columns with the range: $value for the dataframe")

    val rangeColumns = Option(columns).getOrElse("")
    val colArray = rangeColumns.split(",").map(_.trim).filter(_.nonEmpty)
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      parseRangeValue(value) match {
        case Left(e) =>
          logger.error(s"Error applying range filter to the dataframe columns $columns: ${e.getMessage}")
          Left(e)
        case Right((minValue, maxValue)) =>
          Try {
            val keepCondition = rangeKeepCondition(rangeColumns, value).getOrElse(
              throw new QualityDefaultException(s"Invalid columns ($columns) for the ${ConstantsQuality.FILTER_RANGE_PARAM} rule: " +
                s"it must contain at least one column name"))
            df.filter(keepCondition)
          } match {
            case Success(dfResult) =>
              logger.info(s"Dataframe filtering columns: $columns with values inside the range ($value): " +
                s"min=${minValue.map(_.toString).getOrElse("unbounded")} max=${maxValue.map(_.toString).getOrElse("unbounded")}")
              Right(dfResult)
            case Failure(e) => logger.error(s"Error applying range filter to the dataframe columns $columns, with this range: $value", e)
              Left(new QualityDefaultException(s"Error applying range filter to the dataframe columns $columns, with this range: $value\n" +
                s"Error detail: ${e.getMessage}", e))
          }
      }
    } else {
      val missingColumns = colArray.toSet.diff(dfColumns).mkString(",")
      logger.error(s"A specified column in the parameter 'columns' ($columns) was not found in the DataFrame: $missingColumns")
      Left(new QualityDefaultException(s"A specified column in the parameter 'columns' ($columns) was not found " +
        s"in the DataFrame: $missingColumns"))
    }
  }

  /**
   * Builds the condition that a row must satisfy to be kept by the FILTER_NOT_NULL rule: every one of the given
   * columns must be not null.
   *
   * It never throws: when `columns` does not contain any column name (it is null or empty) it logs the error and
   * returns None, so a caller that only needs the condition (for instance to compute the discarded rows) can
   * degrade gracefully instead of aborting the quality process.
   *
   * @param columns String with names of columns separated by "," the rule applies to.
   * @return Option with the condition that a row must satisfy to be kept, None when `columns` does not contain any column name.
   */
  private def notNullKeepCondition(columns: String): Option[Column] = {
    val colArray = Option(columns).getOrElse("").split(",").map(_.trim).filter(_.nonEmpty)

    if (colArray.isEmpty) {
      logger.error(s"Error building the ${ConstantsQuality.FILTER_NOT_NULL_PARAM} condition: the parameter 'columns' " +
        s"($columns) must contain at least one column name")
      None
    } else {
      if (colArray.length > 1) {
        logger.warn(s"The ${ConstantsQuality.FILTER_NOT_NULL_PARAM} rule applies to every column of '$columns' with " +
          s"AND semantics: a row is kept only when all of them are not null, so a row with a null in any single one " +
          s"of them is discarded.")
      }

      Some(colArray.map(safeCol(_).isNotNull).reduce(_ && _))
    }
  }

  /**
   * This method filters the DataFrame `df` keeping the rows whose columns specified by `columns` are not null.
   * It is the way to discard incomplete rows, for instance the metadata of a corrupted file whose measures could
   * not be computed and arrive as null while the rest of the row is correctly filled.
   *
   * Unlike `filterEqualDataFrame`, which keeps a row when ANY of the columns matches, this rule keeps a row only
   * when ALL of the given columns are not null (AND semantics), because it is a quality check.
   *
   * An empty string ("") is NOT null for this rule: null is null and "" is a value, so a row whose checked column
   * holds an empty string is kept. To discard the rows with an empty value, use the FILTER_NOT_EQUAL rule with an
   * empty `value`.
   *
   * The `value` parameter of the rule is not used: the check needs no configuration, so in `metadata.quality`
   * (where it is nullable) it can be left null or empty. Any value informed there is ignored.
   *
   * The rule checks SQL nulls only, never sentinel values: it guarantees that the measure is PRESENT, not that it is
   * usable. A NaN, an Infinity, or the texts "NaN", "nan", "None" or "null" written by an origin are not null values,
   * so the row is kept. This is relevant in practice: the landing of these datasets is a CSV written with the
   * `csv.DictWriter` of Python, where a `None` is written as an empty field that Spark reads as null (so the rule
   * does discard that row), but a `float('nan')` is written as the text `nan`, which is not null and passes the
   * check. To guarantee that a numeric measure is usable and not merely present, combine this rule with FILTER_RANGE,
   * which casts to DoubleType and discards the non finite values. It is a deliberate difference with FILTER_RANGE:
   * filtering NaN inside a rule named NOT_NULL would be confusing.
   *
   * Nested paths are NOT supported: `fields = "metrics.pixel_mean"` is interpreted as a top level column with that
   * literal name (because of the backticks added by `safeCol`), it does not exist, and the rule returns a Left, so it
   * fails loudly. Note also that `isNotNull` over a struct column is true even when every one of its inner fields is
   * null, so pointing the rule to a struct does not check its content: that one is a silent false negative.
   *
   * @param df      DataFrame to filter.
   * @param columns String with names of columns separated by "," to filter.
   * @param value   Not used by this rule, it is ignored. It is kept in the signature to share it with the rest of the filter rules.
   * @return Either-Object with the filtered DataFrame in case of success, or an Exception in case of failure.
   *         It returns an Exception if a specified column in the parameter `columns` was not found in the DataFrame
   *         or if the parameter `columns` does not contain any column name.
   * @throws QualityDefaultException It is raised when it is not possible to apply the filter to the DataFrame.
   */
  def filterNotNullDataFrame(df: DataFrame, columns: String, value: String): Either[Throwable, DataFrame] = {
    logger.info(s"Starting filter NotNull in the columns: $columns for the dataframe")

    val notNullColumns = Option(columns).getOrElse("")
    val colArray = notNullColumns.split(",").map(_.trim).filter(_.nonEmpty)
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      Try {
        val keepCondition = notNullKeepCondition(notNullColumns).getOrElse(
          throw new QualityDefaultException(s"Invalid columns ($columns) for the ${ConstantsQuality.FILTER_NOT_NULL_PARAM} rule: " +
            s"it must contain at least one column name"))
        df.filter(keepCondition)
      } match {
        case Success(dfResult) =>
          logger.info(s"Dataframe filtering columns: $columns keeping the rows where all of them are not null")
          Right(dfResult)
        case Failure(e) => logger.error(s"Error applying not null filter to the dataframe columns $columns", e)
          Left(new QualityDefaultException(s"Error applying not null filter to the dataframe columns $columns\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    } else {
      val missingColumns = colArray.toSet.diff(dfColumns).mkString(",")
      logger.error(s"A specified column in the parameter 'columns' ($columns) was not found in the DataFrame: $missingColumns")
      Left(new QualityDefaultException(s"A specified column in the parameter 'columns' ($columns) was not found " +
        s"in the DataFrame: $missingColumns"))
    }
  }

  /**
   * Method to replace the values of one or more columns in a DataFrame based on the new values provided in JSON Form.
   * It converts values specified in JSON format into Spark SQL expressions for each given column and adds them to the DataFrame.
   * The function takes a DataFrame, a comma-separated list of column names, and a JSON Form of new values.
   *
   * @param df      The DataFrame to replace columns in.
   * @param columns A comma-separated string of the column names that should be replaced.
   * @param newValue A JSON formatted string represent for a start and if column is equal to that value return the new value to update column.
   * @return Either containing DataFrame with updated column values on Right or exception on Left.
   */
  def replaceColumnWhen(df: DataFrame, columns: String, newValue: String): Either[Throwable, DataFrame] = {
    logger.info(s"Replace columns $columns with the value $newValue in the datframe")

    implicit val formats: DefaultFormats.type = DefaultFormats

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      Try {
        val jsonMap = parse(newValue).extract[Map[String, String]]
        colArray.foldLeft(df) { (dfQua, field) =>
          val otherWiseExpr = if (jsonMap.contains("otherwise")) lit(jsonMap("otherwise")) else safeCol(field)

          val columnExpr = jsonMap.foldLeft(otherWiseExpr) { (updatedCol, keyValue) =>
            when(safeCol(field) === keyValue._1, keyValue._2).otherwise(updatedCol)
          }
          dfQua.withColumn(field, safeCol(field).cast(StringType)).withColumn(field, columnExpr)
        }
      } match {
        case Success(dfResult) =>
          logger.info(s"Replace value in the column $columns with value $newValue in the dataframe")
          Right(dfResult)
        case Failure(e) => logger.error(s"Error to replace columns: $columns with the value $newValue in the dataframe", e)
          Left(new QualityDefaultException(s"Error to replace columns: $columns with the value $newValue in the dataframe}\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    } else {
      logger.error("A specified column in the parameter 'columns' was not found in the DataFrame")
      Left(new QualityDefaultException("A specified column in the parameter 'columns' was not found in the DataFrame"))
    }
  }

  /**
   * Replaces null values in the specified column(s) of a DataFrame with the specified value.
   *
   * @param df The DataFrame to process.
   * @param columns The name of the column(s) to replace null values in.
   *                If "*" is used, null values are replaced in all columns of the DataFrame.
   *                Otherwise, it should be a comma-separated string of column names.
   * @param newValue The value to replace null values with.
   * @return Right with the updated DataFrame if successful, or Left with an Exception if not.
   */
  def fillNulls(df: DataFrame, columns: String, newValue: String): Either[Throwable, DataFrame] = {
    logger.info(s"Replace null's in the dataframe with the value: $newValue")

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns) || columns.equals("*")) {
      Try(
        if (columns == "*") {
          df.na.fill(newValue)
        } else {
          val colsSeq: Seq[String] = columns.split(",").toSeq
          df.na.fill(newValue, colsSeq)
        }
      ) match {
        case Success(dfResult) =>
          logger.info(s"Replace value null in the column $columns with value $newValue in the dataframe")
          Right(dfResult)
        case Failure(e) => logger.error(s"Error to replace columns: $columns with the null value for the new Value: $newValue in the dataframe", e)
          Left(new QualityDefaultException(s"Error to replace columns: $columns with the null value for the new Value: $newValue in the dataframe}\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    } else {
      logger.error("A specified column in the parameter 'columns' was not found in the DataFrame")
      Left(new QualityDefaultException("A specified column in the parameter 'columns' was not found in the DataFrame"))
    }
  }

  /**
   * Drops duplicate rows from the specified columns of a DataFrame.
   *
   * @param df The DataFrame to process.
   * @param columns The name(s) of the column(s) to remove duplicates from.
   *                 If null or empty, duplicates are identified and removed from all columns.
   *                 If "*" is used, duplicates are removed from all columns of the DataFrame.
   *                 Otherwise, it should be a comma-separated string of column names.
   * @return Right with the updated DataFrame if successful, or Left with an Exception if not.
   */
  def dropDuplicates(df: DataFrame, columns: String): Either[Throwable, DataFrame] = {
    logger.info(s"Drop duplicate in the column $columns for the dataframe")

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns) || columns.equals("*")) {
      Try(
        if (columns.isEmpty || columns == "*") {
          df.dropDuplicates()
        } else {
          df.dropDuplicates(colArray)
        }
      ) match {
        case Success(dfResult) =>
          logger.info(s"Duplicates droped in the column: $columns in the dataframe")
          Right(dfResult)
        case Failure(e) => logger.error(s"Error to drop duplicate in the column: $columns in the dataframe", e)
          Left(new QualityDefaultException(s"Error to drop duplicate in the column: $columns in the dataframe}\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    } else {
      logger.error("A specified column in the parameter 'columns' was not found in the DataFrame")
      Left(new QualityDefaultException("A specified column in the parameter 'columns' was not found in the DataFrame"))
    }
  }

  /**
   * Converts string-typed columns to Spark TimestampType columns using a specified date format.
   *
   * Each specified column is parsed according to the provided `dateFormat` pattern
   * (which must match the current string representation of the date/time values).
   * The resulting columns are of type TimestampType.
   *
   * @param df The DataFrame to process.
   * @param columns Comma-separated list of column names to convert.
   * @param dateFormat The date/time pattern describing the format of the existing string values.
   *                   Example: "yyyy-MM-dd HH:mm:ss"
   * @return Right with the updated DataFrame if all columns exist and conversion succeeds,
   *         or Left with an Exception if any specified column is missing.
   */
  def formatDates(df: DataFrame, columns: String, dateFormat: String = ""): Either[Throwable, DataFrame] = {
    logger.info(s"Start format dates in columns: $columns for dataframe applying UDF")

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      val dfResult = colArray.foldLeft(df) { (tempDF, column) =>
        tempDF.withColumn(column, to_timestamp(safeCol(column), dateFormat)
          .cast(TimestampType)
          .as(column))
      }

      logger.info(s"Format dates correctly into the columns: $columns for the dataframe")
      Right(dfResult)
    } else {
      logger.error(s"A specified column in the parameter 'columns' was not found in the DataFrame: $columns")
      Left(new QualityDefaultException(s"A specified column in the parameter 'columns' was not found in the DataFrame: $columns"))
    }
  }

  /**
   * Applies a regex pattern to a DataFrame and replace it by a specific value
   *
   * @param df DataFrame to apply the regex pattern
   * @param colNames String with column names separated by commas
   * @param pattern String with the regex pattern used to identify the value
   * @param replacement String with the value that replaces the values found through the pattern
   * @return Either[Throwable, DataFrame] with the resulted DataFrame after applying the regex pattern or an error Throwable
   */
  def regexp(df: DataFrame, colNames: String, pattern: String, replacement: String): Either[Throwable, DataFrame] = {
    val colArray = colNames.split(",").map(_.trim)
    val dfColumns = df.columns.toSet
    logger.info(s"Applying regexp to the columns: ${colArray.mkString(", ")}")
    logger.info(s"Pattern applied to the columns: $pattern")
    logger.info(s"Replacement value applied to the columns: $replacement")

    if (colArray.toSet.subsetOf(dfColumns) && pattern.nonEmpty) {
      Try(colArray.foldLeft(df) { (updatedDF, colName) =>
        updatedDF.withColumn(colName, regexp_replace(safeCol(colName), pattern, replacement))
      }) match {
        case Success(dfResult) =>
          logger.info(s"RegExp applied successfully to the columns: $colNames in the dataframe")
          Right(dfResult)
        case Failure(e) =>
          logger.error(s"Error to apply RegExp in the columns: $colNames for the dataframe", e)
          Left(new QualityDefaultException(s"Error to apply RegExp in the columns: $colNames for the dataframe}\n" +
            s"Error detail: ${e.getMessage}", e))
      }

    } else {
      logger.error(s"A specified column in the parameter 'columns' was not found in the DataFrame: $colNames")
      Left(new QualityDefaultException(s"A specified column in the parameter 'columns' was not found in the DataFrame: $colNames"))
    }
  }

  /**
   * Flattens a JSON string column by parsing its structure and expanding its fields
   * into separate top-level columns within the DataFrame.
   *
   * The function:
   *  - Validates that the specified column is of type `StringType`.
   *  - Infers the JSON schema from the first non-null record.
   *  - Optionally removes enclosing brackets if the JSON is stored as a single-element array.
   *  - Applies the inferred schema and creates new columns for each JSON field.
   *  - Drops the original JSON column after expansion.
   *
   * Returns the resulting DataFrame wrapped in an `Either` to handle parsing or schema errors safely.
   *
   * @param df Input DataFrame containing the JSON column.
   * @param colName Name of the JSON string column to be flattened.
   * @return Either an error (Left) or the transformed DataFrame with flattened JSON fields (Right).
   */
    def flattenJsonField(df: DataFrame, colName: String): Either[Throwable, DataFrame] = {
      logger.info(s"Applying Explode Json Field in the column: $colName")

      try {
        val fieldDataType = df.schema(colName).dataType
        if (fieldDataType.isInstanceOf[StringType]) {
          logger.info(s"Getting the JSON schema")
          val schema = getJsonSchema(df.filter(safeCol(colName).isNotNull).select(safeCol(colName)).first().getString(0))

          logger.info(s"Converting the JSON array to a JSON object")
          val dfWithObject = df.withColumn(colName, regexp_replace(safeCol(colName), "\\[|\\]", ""))

          logger.info(s"Verifying if colName is a JSON field")
          val parsedDF = applyJsonSchemaToStringColumn(dfWithObject, colName, schema)

          val finalDF = schema.fields.foldLeft(parsedDF) { (tempDF, f) =>
            tempDF.withColumn(s"${colName}_${f.name}", col(s"$colName.${f.name}"))
          }
          Right(finalDF.drop(colName))
        } else {
          logger.error(s"The field '$colName' should be of StringType")
          Left(new QualityDefaultException(s"The field '$colName' should be of StringType"))
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error while exploding field '$colName'", e)
          Left(new QualityDefaultException(s"Error while exploding field '$colName'}\n" +
            s"Error detail: ${e.getMessage}", e))
      }
    }

  /**
   * Flattens a nested StructType column into individual top-level columns in the given DataFrame.
   *
   * For each field within the specified struct column, a new column is created at the top level.
   * The original struct column is dropped after expansion.
   *
   * This function:
   *  - Validates that the provided column is of type `StructType`.
   *  - Iterates through each field of the struct and extracts it as a new column.
   *  - Handles column names safely using backticks to support special characters.
   *  - Returns the resulting DataFrame wrapped in an `Either` for safe error handling.
   *
   * @param df Input DataFrame containing the struct column.
   * @param colName Name of the StructType column to be flattened.
   * @return Either an error (Left) or the transformed DataFrame with flattened fields (Right).
   */
  def flattenStructField(df: DataFrame, colName: String): Either[Throwable, DataFrame] = {
    logger.info(s"Applying Explode Struct Field in the column: $colName")

    try {
      val fieldDataType = df.schema(colName).dataType
      if (fieldDataType.isInstanceOf[StructType]) {
        logger.info(s"Applying explode in column: $colName for structType ${fieldDataType.typeName}")

        val structType = fieldDataType.asInstanceOf[StructType]
        val structFields = structType.fields.map(_.name)

        val explodedDF = structFields.foldLeft(df) { (tempDF, f) =>
          tempDF.withColumn(f, col(s"`$colName`.`$f`"))
        }.drop(colName)

        Right(explodedDF)
      } else {
        logger.error(s"The field '$colName' should be of StructType")
        Left(new QualityDefaultException(s"The field '$colName' should be of StructType"))
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error while exploding field '$colName'", e)
        Left(new QualityDefaultException(
          s"Error while exploding field '$colName'\nError detail: ${e.getMessage}", e
        ))
    }
  }

  /**
   * Adds year, month and day columns based on a date column.
   *
   * @param df      DataFrame containing the Date column
   * @param dateColumn String with the name of the Data column
   * @return Either[Throwable, DataFrame] with the resulted DataFrame after the year, month and day columns have been added or an error Throwable
   */
  def addDateColumns(df: DataFrame, dateColumn: String): Either[Throwable, DataFrame] = {
    Try {
      val dfWithTimestamp = df.withColumn(dateColumn, to_timestamp(safeCol(dateColumn)))
      // Extract year, month, and day from the date column
      val dfWithYearMonthAndDay = dfWithTimestamp
        .withColumn("Year", year(safeCol(dateColumn)))
        .withColumn("Month", month(safeCol(dateColumn)))
        .withColumn("Day", dayofmonth(safeCol(dateColumn)))
      // Return the resulting DataFrame
      dfWithYearMonthAndDay
    } match {
      case Success(dfResult) =>
        logger.info(s"Year, month and day columns were added to the dataframe (based on $dateColumn column")
        Right(dfResult)
      case Failure(e) =>
        logger.error(s"Error trying to add year, month and day columns based on $dateColumn column", e)
        Left(new QualityDefaultException(s"Error trying to add year, month and day columns based on $dateColumn column}\n" +
          s"Error detail: ${e.getMessage}", e))
    }
  }

  /**
   * Formats date or timestamp columns in a DataFrame into string representations
   * using the specified date/time pattern.
   *
   * Each specified column is transformed with the `date_format` function,
   * converting its values to string type according to the provided `dateFormat`.
   *
   * @param df The DataFrame to process.
   * @param columns Comma-separated list of column names to format.
   * @param dateFormat The desired output date/time format pattern.
   *                   Example: "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
   * @return Right with the updated DataFrame if all columns exist and conversion succeeds,
   *         or Left with an Exception if any specified column is missing.
   */
  def formatDatesAsString(df: DataFrame, columns: String, dateFormat: String = ""): Either[Throwable, DataFrame] = {
    logger.info(s"Start format dates in columns: $columns for dataframe applying UDF")

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      val dfResult = colArray.foldLeft(df) { (tempDF, column) =>
        tempDF.withColumn(column, date_format(safeCol(column), dateFormat)
          .as(column))
      }

      logger.info(s"Format dates correctly into the columns: $columns for the dataframe")
      Right(dfResult)
    } else {
      logger.error(s"A specified column in the parameter 'columns' was not found in the DataFrame: $columns")
      Left(new QualityDefaultException(s"A specified column in the parameter 'columns' was not found in the DataFrame: $columns"))
    }
  }

  /**
   * Formats date or timestamp columns in a DataFrame into new string columns.
   *
   * For each specified column, a new column is created with the suffix "_as_str",
   * containing the formatted date/time value as a string, based on the provided `dateFormat` pattern.
   *
   * Example:
   *   If the column "event_date" exists, the function will create "event_date_as_str"
   *   containing the formatted string representation of the timestamp.
   *
   * @param df The DataFrame to process.
   * @param columns Comma-separated list of column names to format.
   * @param dateFormat The output date/time format pattern to apply.
   *                   Example: "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
   * @return Right with the updated DataFrame if all columns exist and formatting succeeds,
   *         or Left with an Exception if any specified column is missing.
   */
  def formatDatesAsStringNew(df: DataFrame, columns: String, dateFormat: String = ""): Either[Throwable, DataFrame] = {
    logger.info(s"Start format dates in columns: $columns for dataframe applying UDF")

    val colArray = columns.split(",")
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      val dfResult = colArray.foldLeft(df) { (tempDF, column) => {
        val newColumnName = s"${column}_as_str"
        tempDF
          .withColumn(column, safeCol(column).cast(TimestampType))
          .withColumn(newColumnName, date_format(safeCol(column), dateFormat)
          .as(column))
      }
      }

      logger.info(s"Format dates correctly into the columns: $columns for the dataframe")
      Right(dfResult)
    } else {
      logger.error(s"A specified column in the parameter 'columns' was not found in the DataFrame: $columns")
      Left(new QualityDefaultException(s"A specified column in the parameter 'columns' was not found in the DataFrame: $columns"))
    }
  }

  /**
   * Explode a specific delimited column in multiple rows.
   *
   * @param df The DataFrame to process
   * @param columns Name of the columns containing delimited values
   * @param delimiter Delimiter
   * @return DataFrame with one row for each separated value
   */
  def explodeDelimitedColumn(df: DataFrame, columns: String, delimiter: String): Either[Throwable, DataFrame] = {
    logger.info(s"Starting explode operation for columns: $columns using delimiter: '$delimiter'")

    val colArray = columns.split(",").map(_.trim).filter(_.nonEmpty)
    val dfColumns = df.columns.toSet

    if (colArray.toSet.subsetOf(dfColumns)) {
      Try {
        val explodedDf = colArray.foldLeft(df) { (tempDf, colName) =>
          tempDf.withColumn(colName, explode(split(safeCol(colName), delimiter)))
        }
        logger.info(s"Successfully exploded columns: ${colArray.mkString(", ")}")
        explodedDf
      } match {
        case Success(resultDf) => Right(resultDf)
        case Failure(e) =>
          logger.error(s"Error applying explode on columns: $columns", e)
          Left(new QualityDefaultException(
            s"Error applying explode on columns: $columns\nDetail: ${e.getMessage}", e
          ))
      }
    } else {
      logger.error(s"One or more specified columns in 'columns' ($columns) were not found in the DataFrame")
      Left(new QualityDefaultException(
        s"One or more specified columns in 'columns' ($columns) were not found in the DataFrame"
      ))
    }
  }

  /**
   * Convierte columnas con epoch Unix a timestamp según la unidad indicada
   * (seconds, millis o micros). Valores no numéricos quedan a null (try_cast).
   *
   * @param df      DataFrame de entrada
   * @param columns columnas a convertir, separadas por comas
   * @param unit    unidad del epoch: seconds|millis|micros (con alias habituales)
   * @return Right con el DataFrame convertido, o Left con el error
   */
  def unixToTimestamp(df: DataFrame, columns: String, unit: String): Either[Throwable, DataFrame] = {

    logger.info(s"Start unixToTimestamp in columns: $columns with unit=$unit")

    val cols = columns.split(",").map(_.trim).filter(_.nonEmpty)
    val dfCols = df.columns.toSet

    if (!cols.toSet.subsetOf(dfCols)) {
      val missing = cols.filterNot(dfCols.contains).mkString(", ")
      val msg = s"Columns not found in DataFrame: $missing"
      logger.error(msg)
      Left(new QualityDefaultException(msg))
    } else {
      val fn = unit.toLowerCase match {
        case "seconds" | "second" | "sec" | "s" => "timestamp_seconds"
        case "millis" | "millisecond" | "ms" => "timestamp_millis"
        case "micros" | "microsecond" | "us" => "timestamp_micros"
        case other =>
          val msg = s"Unsupported unit '$other'. Use: seconds|millis|micros"
          logger.error(msg)
          return Left(new IllegalArgumentException(msg))
      }

      val resultDf = cols.foldLeft(df) { (acc, c) =>
        val cc = s"`$c`"
        // try_cast: si viene como string no numérico o vacío -> null
        acc.withColumn(c, expr(s"$fn(try_cast($cc AS BIGINT))"))
      }
      logger.info(s"unixToTimestamp applied correctly to columns: ${cols.mkString(",")}")
      Right(resultDf)
    }
  }
}
