package com.gestamp.hypatia.postgresql.auth

import com.gestamp.hypatia.postgresql.auth.Credentials.getCredentials
import com.gestamp.hypatia.postgresql.dao.{DatasetDao, DomainDao, SubdomainDao}
import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

class PostgreSqlManagerTest extends FunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /*test("Set properties") {
    val properties: Properties = new Properties()
    getCredentials().foreach{ case (k, v) => properties.setProperty(k, v.toString)}
    properties
  }

  test("testing Schemas") {
    val sch = new SchemasDao
    val result = sch.getSchemaById(1)
    logger.info(s"Schema: ${result.schema}")
  }


  test("testing Domains") {
    val dom = new DomainsDao
    val result = dom.getDomainByCode("ttd")
    logger.info(s"Schema: ${result.description}")
  }


  test("testing Subdomains") {
    val sub = new SubdomainsDao
    val result = sub.getSubdomainByCode("ttsd")
    logger.info(s"Schema: ${result.description}")
  }*/


}
