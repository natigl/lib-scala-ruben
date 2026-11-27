package com.gestamp.hypatia.integration.operations

import com.gestamp.hypatia.integration.exception.IntegrationManagerException
import com.gestamp.hypatia.integration.utils.ConstantsIntegration.JOIN_OPERATIONS
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object TaxonomyOperations extends ITaxonomyOperations {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Une todos los DataFrames por nombre de columna (unionByName con
   * allowMissingColumns): las columnas que falten en alguno quedan a null.
   *
   * @param dataFrames DataFrames a unir (al menos uno)
   * @return el DataFrame unión
   * @throws IntegrationManagerException si la lista está vacía o la unión falla
   */
  def unionByNameOperation(dataFrames: Seq[DataFrame]): DataFrame = {
    logger.info(s"Run UnionByName operation")

    if (dataFrames.isEmpty) {
      throw new IntegrationManagerException("At least one DataFrame is required for unionByName operation")
    }

    Try(dataFrames.tail.foldLeft(dataFrames.head) { (df1, df2) =>
      df1.unionByName(df2, allowMissingColumns=true)
    }) match {
      case Success(dfResult) => dfResult
      case Failure(exception) =>
        throw new IntegrationManagerException("UnionByName operation failed", exception)
    }
  }

  /**
   * Une todos los DataFrames por posición de columna (union clásica); requiere
   * que todos tengan el mismo esquema.
   *
   * @param dataFrames DataFrames a unir (al menos uno)
   * @return el DataFrame unión
   * @throws IntegrationManagerException si la unión falla
   */
  def unionOperation(dataFrames: Seq[DataFrame]): DataFrame = {
    logger.info(s"Run Union operation")

    if (dataFrames.isEmpty) {
      throw new IllegalArgumentException("At least one DataFrame is required for union operation")
    }

    Try(dataFrames.tail.foldLeft(dataFrames.head) { (df1, df2) =>
      df1.union(df2)
    }) match {
      case Success(dfResult) => dfResult
      case Failure(exception) =>
        throw new IntegrationManagerException("Union operation failed", exception)
    }
  }

  /**
   * Hace join de los DataFrames con el tipo indicado y la expresión de igualdad
   * "campo1 = campo2 AND ...". Si ambos lados usan los mismos nombres de columna,
   * el join se hace por secuencia de columnas (evita duplicarlas).
   *
   * @param operation  tipo de join (inner, left, right, ...)
   * @param expr       expresión de igualdad de campos separada por AND
   * @param dataFrames DataFrames a combinar
   * @return el DataFrame resultante del join
   * @throws IntegrationManagerException si el tipo de join no es válido
   */
  def joinOperation(operation: String, expr: String, dataFrames: Seq[DataFrame]): DataFrame = {
    logger.info(s"Run $operation join operation")

    val joinType = if (JOIN_OPERATIONS.contains(operation.toLowerCase)) operation.toLowerCase
    else throw new IntegrationManagerException(s"Invalid join type: $operation")

    val routeExprs = expr.split(" AND ")

    // Crear una lista de tuplas que contengan los campos a unir
    val joinFields = routeExprs.map { expr =>
      val fields = expr.replaceAll(" ", "").split("=")
      (fields(0), fields(1))
    }

    val joinWithSeq = joinFields.forall { case (field1, field2) => field1 == field2 }

    if (joinWithSeq) {
      val joinColumns = joinFields.map(_._1)
      dataFrames.reduce((df1, df2) => df1.join(df2, joinColumns, joinType))
    } else {
      // Crear una lista de condiciones de unión
      val joinConditions = joinFields.map { case (field1, field2) =>
        dataFrames.head(field1) === dataFrames(1)(field2)
      }

      // Combinar todas las condiciones de unión en una sola usando el operador &&
      val finalJoinCondition = joinConditions.tail.foldLeft(joinConditions.head) { (condition, nextCondition) =>
        condition && nextCondition
      }

      dataFrames.reduce((df1, df2) => df1.join(df2, finalJoinCondition, joinType))
    }
  }

}
