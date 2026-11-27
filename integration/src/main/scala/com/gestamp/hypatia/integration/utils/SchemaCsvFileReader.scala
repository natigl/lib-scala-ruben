package com.gestamp.hypatia.integration.utils

import cats.Show.Shown.mat
import com.gestamp.hypatia.adls.{BlobStorageManager, ConstructorPaths}
import com.gestamp.hypatia.integration.exception.IntegrationManagerException
import com.gestamp.hypatia.spark.{SparkConfiguration, SparkOptions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

/**
 * Object that contains all methods to automatize the schema cast defined in a .csv schema file
 * input:
 *  - source = raw_clean schema source
 *  - tableDistillation = Delta Table name
 * output:
 *  - StructType with the schema defined in the .csv file
 */
object SchemaCsvFileReader {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Method that casts a DataFrame object according to a csv schema.
   * Casting is done by position under the premise:
   *      - "The order of the columns in a DataFrame in Spark will always default to the order in which are defined in the Delta table."
   * We use the zipWithIndex function to obtain both the name of each column of the dfRawClean DataFrame and its numerical index
   * @param source             the name of the source.
   * @param tableDistillation  the name of the Delta table.
   * @return the Raw Clean DataFrame object cast according to the csv schema.
   */
  def castDataFrameByCSVSchema(source: String, tableDistillation: String, dfRawClean: DataFrame): DataFrame = {
    logger.info(s"Start schema casting a DataFrame for the Delta Table: $tableDistillation")

    val pathCSVSchema = s"source/${source.toLowerCase}/${tableDistillation.toLowerCase}"
    val silverSchemaCDF: StructType = processCsvFile(source.toLowerCase, tableDistillation.toLowerCase)

    try {
      //TODO: Cast propiedad nullable
      val newColumns = dfRawClean.columns.zipWithIndex.map {
        case (colName, columnOrder) =>
          val currentStructField = silverSchemaCDF(columnOrder)
          val castedTypeColumn = col(s"`$colName`").cast(currentStructField.dataType)
          castedTypeColumn.alias(currentStructField.name)
      }

      dfRawClean.select(newColumns:_*)
    } catch {
      case e: Exception =>
        logger.error(s"Error while casting schema to DataFrame for the Delta Table: '$tableDistillation'", e)
        throw new IntegrationManagerException(s"Error while casting schema to DataFrame for the Delta Table: '$tableDistillation'", e)
    }
  }

  /**
   * Method that converts the schema defined in the CSV to a StructType
   * - It's important to add the fields to the Change Data Feed.
   *
   * @param pathCSVSchema the path where the csv schema is located.
   * @return a StructType object containing the schema cast defined in the .csv file.
   */
  private def processCsvFile(source: String, tableDistillation: String): StructType = {
    logger.info(s"Start process (read and cast to StructType) CSV file in the table: $source $tableDistillation")

    val schemaLines = readCsvSchemaFile(source, tableDistillation)
    val schemaFieldsPlusCDF = addFieldsForChangeDataFeed(schemaLines)

    val fields = schemaFieldsPlusCDF.map(parseField)

    StructType(fields)
  }

  private def readCsvSchemaFile(source: String, tableDistillation: String): List[String] = {
  try {
    val blobPath = ConstructorPaths.buildSchemasPath(source, tableDistillation)
 
    logger.info(s"Start reading CSV file in path: /$blobPath")
 
    val spark = org.apache.spark.sql.SparkSession.builder().getOrCreate()
 
    val lines = spark.sparkContext
      .textFile(blobPath)
      .collect()
      .toList
 
    lines
  } catch {
    case e: Exception =>
      throw new IntegrationManagerException(
        s"Failed to read CSV schema at table: $tableDistillation. Exception message: ${e.getMessage}.",
        e
      )
  }
}

/*
  /**
   * Private method that reads the csv file and returns a list with the lines of the csv schema.
   *
   * @param pathCSVSchema the path of the csv schema file.
   * @return a list with the lines of the csv schema.
   */
  private def readCsvSchemaFile(source: String, tableDistillation: String): List[String] = {
    try {
      val blobPath = ConstructorPaths.buildSchemasPath(source, tableDistillation)

      logger.info(s"Start reading CSV file in path: /$blobPath")

      val sparkOptions = SparkOptions(fileFormat = "csv", otherOptions = Map("header" -> "false"))
      val df = BlobStorageManager.readCsv(blobPath, sparkOptions)

      val lines = df.collect().map(_.toSeq.mkString(",")).toList

      lines
    } catch {
      case e: Exception =>
        throw new IntegrationManagerException(s"Failed to read CSV schema at table: $tableDistillation. Exception message: ${e.getMessage}.", e)
    }
  }*/

  /**
   * A utility function that add another 3 fields "_change_type", "_commit_version" and "_commit_timestamp"
   * used later to maintain a Change Data Feed in the destination table.
   *
   * @param schemaLines a list with each line in the .csv schema.
   * @return a list with all the lines in the csv schema with the additional CDF fields.
   */
  private def addFieldsForChangeDataFeed(schemaLines: List[String]): List[String] = {
    schemaLines :+ "_change_type,String,true" :+ "_commit_version,Long,true" :+ "_commit_timestamp,Timestamp,true"
  }

  /**
   * Transforms one String object into a StructField object based on the defined .csv schema, it throws an IllegalArgumentException
   * if the dataType is not recognised or an ArrayIndexOutOfBoundsException if line is not well formatted.
   *
   * @param line a line form the csv schema to be transformed.
   * @return a StructField object based on the csv schema for the line passed as parameter.
   */
  //TODO: Revisar los tipos complejos:
  // MapType = MapType(StringType, IntegerType, ....¿?)
  // ArrayType = ArrayType(StringType, StringType, ....¿?)
  private def parseField(line: String): StructField = {
    try {
      val Array(name, dataType, nullable) = line.split(",")
      val fieldDataType = dataType match {
        case ConstantsIntegration.BINARY         => BinaryType
        case ConstantsIntegration.FLOAT          => FloatType
        case ConstantsIntegration.STRING         => StringType
        case ConstantsIntegration.INTEGER        => IntegerType
        case ConstantsIntegration.DOUBLE         => DoubleType
        case ConstantsIntegration.BOOLEAN        => BooleanType
        case ConstantsIntegration.LONG           => LongType
        case ConstantsIntegration.SHORT          => ShortType
        case ConstantsIntegration.DATE           => DateType
        case ConstantsIntegration.TIMESTAMP      => TimestampType
        //case ConstantsIntegration.STRUCT         => StructType
        //case ConstantsIntegration.MAP            => MapType
        //case ConstantsIntegration.ARRAY          => ArrayType
        case ConstantsIntegration.DECIMAL_16_4   => DecimalType(16, 4)
        case ConstantsIntegration.DECIMAL_38_18  => DecimalType(38, 18)
        case ConstantsIntegration.DECIMAL_38_4   => DecimalType(38, 4)
        case ConstantsIntegration.DECIMAL_18_4   => DecimalType(18, 4)
        case ConstantsIntegration.DECIMAL_16_5   => DecimalType(16, 5)
        case ConstantsIntegration.BYTE           => ByteType
        case _ =>
          throw new IllegalArgumentException(s"Unknown data type: $dataType")
      }
      val fieldNullable = java.lang.Boolean.valueOf(nullable)
      StructField(name, fieldDataType, fieldNullable)
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        logger.error(s"Failed to parse line: $line. Error message: ${e.getMessage}")
        throw new IntegrationManagerException(s"Failed to parse line: $line. Error message: ${e.getMessage}")
    }
  }

}
