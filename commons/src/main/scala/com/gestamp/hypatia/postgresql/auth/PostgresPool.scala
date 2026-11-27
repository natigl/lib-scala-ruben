package com.gestamp.hypatia.postgresql.auth

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Connection

object PostgresPool {
  private val credsMetadata = Credentials.getCredentials("metadata")
  private val credsOrchestration = Credentials.getCredentials("orchestration")
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Config Metadata
  private val hikariConfigMetadata = new HikariConfig()
  hikariConfigMetadata.setJdbcUrl(credsMetadata("url"))
  hikariConfigMetadata.setUsername(credsMetadata("user"))
  hikariConfigMetadata.setPassword(credsMetadata("password"))
  hikariConfigMetadata.setMaximumPoolSize(5)
  hikariConfigMetadata.setPoolName(credsMetadata("poolName"))

  lazy val dataSourceMetadata: HikariDataSource = new HikariDataSource(hikariConfigMetadata)
  logger.info(s"Conectando a la base de datos con URL: ${credsMetadata("url")}")

  // Config Orchestration
  private val hikariConfigOrchestration = new HikariConfig()
  hikariConfigOrchestration.setJdbcUrl(credsOrchestration("url"))
  hikariConfigOrchestration.setUsername(credsOrchestration("user"))
  hikariConfigOrchestration.setPassword(credsOrchestration("password"))
  hikariConfigOrchestration.setMaximumPoolSize(5)
  hikariConfigOrchestration.setPoolName(credsOrchestration("poolName"))

  lazy val dataSourceOrchestration: HikariDataSource = new HikariDataSource(hikariConfigOrchestration)
  logger.info(s"Conectando a la base de datos con URL: ${credsOrchestration("url")}")

  // Get Pool connection
  def getConnection(db: String): Connection = db match {
    case "metadata" => dataSourceMetadata.getConnection()
    case "orchestration" => dataSourceOrchestration.getConnection()
    case _ => throw new IllegalArgumentException(s"Unknown database: $db")
  }

  // Closing connections (Unused)
  def closeConnections(): Unit = {
    logger.info("Closing HikariCP connections...")
    if (!dataSourceMetadata.isClosed) {
      dataSourceMetadata.close()
      logger.info("Pool Metadata closed.")
    }
    if (!dataSourceOrchestration.isClosed) {
      dataSourceOrchestration.close()
      logger.info("Pool Orchestration closed.")
    }
  }
}
