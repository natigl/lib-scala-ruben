package com.gestamp.hypatia.postgresql.auth

import com.gestamp.hypatia.postgresql.auth.Credentials.getCredentials
import org.slf4j.{Logger, LoggerFactory}
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import scala.util.{Failure, Success, Try}

/**
 * Base de todos los DAO de PostgreSQL: gestiona la conexión (pool HikariCP por esquema)
 * y ofrece las primitivas runSelect/runInsert. Cada DAO concreto aporta el mapeo
 * fila → modelo implementando mapperRowdata (y setInsert si inserta).
 *
 * @param schema esquema de la base de datos sobre el que trabaja el DAO (por defecto metadata)
 * @tparam T modelo al que se mapean las filas del ResultSet
 */
abstract class PostgreSqlManager[T](val schema: String = "metadata") {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Construye las Properties de conexión a partir de las credenciales del esquema.
   *
   * @return las propiedades de conexión
   */
  private def setCustomProperties(): Properties = {
    val properties: Properties = new Properties()
    getCredentials(schema).foreach{ case (k, v) => properties.setProperty(k, v)}
    properties
  }

  /**
   * Obtiene una conexión del pool HikariCP asociado al esquema.
   *
   * @return la conexión obtenida
   */
  private def createConnectionPostgreSql(): Connection = {
    Try(PostgresPool.getConnection(schema)) match {
      case Success(conn) =>
        logger.info(s"HikariCP connection obtained for schema: $schema")
        conn
      case Failure(e)  => throw new Exception("Error getting connection Pool using HikariCP", e)
    }
  }

  /**
   * Alias de createConnectionPostgreSql: conexión lista para usar.
   *
   * @return la conexión obtenida del pool
   */
  private def getConnection: Connection = createConnectionPostgreSql()

  /**
   * Devuelve la conexión al pool si sigue abierta.
   *
   * @param conn conexión a liberar
   */
  private def closeConnection(conn: Connection): Unit = {
    if (conn != null && !conn.isClosed) {
      conn.close()
      logger.info("Connection returned to the pool correctly.")
    }
  }

  /**
   * Mapea una fila del ResultSet al modelo T. Lo implementa cada DAO concreto.
   *
   * @param resultSet cursor posicionado en la fila a mapear
   * @return la fila convertida al modelo
   */
  protected def mapperRowdata(resultSet: ResultSet): T

  /**
   * Rellena los parámetros del PreparedStatement de inserción a partir del modelo.
   * Por defecto no está soportado: lo sobrescriben los DAO que insertan.
   *
   * @param preparedStatement statement de inserción a completar
   * @param insertModel       modelo con los valores a insertar
   * @return el statement con los parámetros ya asignados
   */
  def setInsert(preparedStatement: PreparedStatement, insertModel: T): PreparedStatement = {
    throw new UnsupportedOperationException("This method is not implemented")
  }

  /**
   * Ejecuta una SELECT y devuelve las filas mapeadas al modelo. Gestiona la
   * conexión (obtención y devolución al pool) de forma transparente.
   *
   * @param sql sentencia SELECT a ejecutar
   * @return las filas resultantes mapeadas a T
   */
  def runSelect(sql: String): Seq[T] = {
    val conn = getConnection
    try {
      logger.info(s"SQL statement about to be executed: $sql")
      createStatement(conn, sql, mapperRowdata)
    } finally {
      closeConnection(conn)
    }
  }

  /**
   * Ejecuta una INSERT parametrizada rellenando los valores con setInsert.
   * Gestiona la conexión de forma transparente.
   *
   * @param sql         sentencia INSERT con placeholders
   * @param insertModel modelo con los valores a insertar
   */
  def runInsert(sql: String, insertModel: T): Unit = {
    val conn = getConnection
    try {
      logger.info(s"SQL Insert Statement about to be executed: $sql")
      val insertPreparedStatement = this.setInsert(preparedStatement(conn, sql), insertModel)
      logger.info(s"Total rows affected: " + insertPreparedStatement.executeUpdate())
    } finally {
      closeConnection(conn)
    }
  }

  /**
   * Prepara y ejecuta la consulta sobre la conexión dada aplicando el mapper.
   *
   * @param connection    conexión a usar
   * @param sql           sentencia a ejecutar
   * @param mapperRowData función de mapeo fila → modelo
   * @return las filas resultantes mapeadas
   */
  private def createStatement(connection: Connection, sql: String, mapperRowData: ResultSet => T): Seq[T] = {
    execute(preparedStatement(connection, sql), mapperRowData)
  }

  /**
   * Crea el PreparedStatement de la sentencia sobre la conexión dada.
   *
   * @param connection conexión a usar
   * @param sql        sentencia a preparar
   * @return el statement preparado
   */
  private def preparedStatement(connection: Connection, sql: String): PreparedStatement = {
    connection.prepareStatement(sql)
  }

  /**
   * Ejecuta la consulta del statement y materializa el ResultSet en una lista
   * aplicando el mapper a cada fila.
   *
   * @param preparedStatement statement a ejecutar
   * @param mapperRowData     función de mapeo fila → modelo
   * @return las filas resultantes mapeadas
   */
  private def execute[R](preparedStatement: PreparedStatement, mapperRowData: ResultSet => R): Seq[R] = {
    val rs = preparedStatement.executeQuery()
    new MapperResultIterator[R](rs, mapperRowData).getList
  }

}
