package com.gestamp.hypatia.postgresql.auth

import com.gestamp.hypatia.databricks.DatabricksManager

/**
 * Construcción de las credenciales de conexión a PostgreSQL a partir de los
 * secretos de Databricks.
 */
object Credentials {

  /**
   * Construye las credenciales de conexión (url, user, password, poolName) del
   * esquema indicado leyendo los secretos del secret scope de Databricks.
   *
   * @param database esquema destino: metadata (por defecto) u orchestration
   * @return mapa con url JDBC, usuario, contraseña y nombre del pool
   */
  def getCredentials(database: String = "metadata"): Map[String, String] = {
    val databricksManager = new DatabricksManager()
    val poolName = if (database == "orchestration")
      "orchestrationPostgresPool"
    else
      "HypatiaPostgresPool"
    val JDBC_POSTGRE_SECRET = if (database == "orchestration")
      "jdbc:postgresql://" + databricksManager.dbUtilsGetSecret("psql-flexserver-hostname") + ":5432/" + databricksManager.dbUtilsGetSecret("psql-schema-orchestration") + "?ssl=true&sslmode=require"
    else
      "jdbc:postgresql://" + databricksManager.dbUtilsGetSecret("psql-flexserver-hostname") + ":5432/" + databricksManager.dbUtilsGetSecret("psql-schema-metadata") + "?ssl=true&sslmode=require"

    Map(
      "url" -> JDBC_POSTGRE_SECRET,
      "user" -> databricksManager.dbUtilsGetSecret("psql-flexserver-admin"),
      "password" -> databricksManager.dbUtilsGetSecret("psql-flexserver-password"),
      "poolName" -> poolName
    )
  }
}
