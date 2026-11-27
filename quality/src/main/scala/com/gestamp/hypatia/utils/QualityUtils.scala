package com.gestamp.hypatia.utils

import com.gestamp.hypatia.spark.SparkConfiguration
import com.gestamp.hypatia.exceptions.QualityDefaultException
import com.gestamp.hypatia.spark.SparkConfiguration.spark
import com.gestamp.hypatia.utils.ConstantsQuality.KNOWN_FORMATS
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.util.Try

object QualityUtils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Infiere el esquema Spark (StructType) de un string JSON de ejemplo.
   *
   * @param jsonString JSON del que inferir el esquema
   * @return el esquema inferido
   * @throws QualityDefaultException si el JSON está mal formado
   */
  def getJsonSchema(jsonString: String): StructType = {
    val spark = SparkConfiguration.spark
    import spark.implicits._
    val jsonSchema = spark.read.json(Seq(jsonString).toDS()).schema
    if (jsonSchema.fields(0).name == "_corrupt_record") {
      throw new QualityDefaultException("El string a partir del cual se está intentando extraer el schema JSON tiene un formato incorrecto")
    }
    jsonSchema
  }

  /**
   * Parsea una columna string con contenido JSON aplicándole el esquema dado y
   * la sustituye por la columna estructurada resultante. Falla si algún registro
   * no nulo no se puede parsear con ese esquema.
   *
   * @param df         DataFrame de entrada
   * @param columnName columna string con JSON
   * @param schema     esquema a aplicar
   * @return el DataFrame con la columna parseada
   * @throws QualityDefaultException si hay registros que no cumplen el esquema
   */
  def applyJsonSchemaToStringColumn(df: DataFrame, columnName: String, schema: StructType): DataFrame = {
    logger.info(s"Applying JSON schema to the column $columnName")
    val parsedColumn = columnName+"_parsed"
    val dfWithObject = df.withColumn(parsedColumn, from_json(safeCol(columnName), schema))

    val unparsedRecords = dfWithObject
      .filter(safeCol(columnName).isNotNull && safeCol(parsedColumn).isNull)
      .count()

    if (unparsedRecords > 0) {
      logger.info(s"There are records where the indicated schema could not be applied to the JSON of the $columnName column.")
      throw new QualityDefaultException(s"There are records where the indicated schema could not be applied to the JSON of the $columnName column.")
    }
    dfWithObject.drop(columnName).withColumnRenamed(parsedColumn,columnName)
  }

  /** Escape column names containing special characters such as ‘.’ or # */
  def safeCol(columnName: String): Column = col(s"`$columnName`")
}
