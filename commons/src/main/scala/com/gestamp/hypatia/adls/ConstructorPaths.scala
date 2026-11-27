package com.gestamp.hypatia.adls

import org.slf4j.{Logger, LoggerFactory}

object ConstructorPaths {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val sufix: String = ".dfs.core.windows.net"
  private val prefix: String = "abfss://"

  private val adlsLandingPath = s"${prefix}${KeyCredentials.containerNameStaging}@${KeyCredentials.storageAccountNameBronze}$sufix"
  private val adlsRawSourcePath = s"${prefix}${KeyCredentials.containerNameBronze}@${KeyCredentials.storageAccountNameBronze}$sufix/source"
  private val adlsRawCleanPath = s"${prefix}${KeyCredentials.containerNameBronze}@${KeyCredentials.storageAccountNameBronze}$sufix/clean"
  private val adlsSilverDistillationPath = s"${prefix}${KeyCredentials.containerNameSilverDistillation}@${KeyCredentials.storageAccountNameSilver}$sufix"
  private val adlsSilverIntegrationPath = s"${prefix}${KeyCredentials.containerNameSilverIntegration}@${KeyCredentials.storageAccountNameSilver}$sufix"
  private val adlsSilverSchemasPath = s"${prefix}${KeyCredentials.containerNameSilverSchemas}@${KeyCredentials.storageAccountNameSilver}$sufix/integration"
  private val adlsSilverSchemasSemanticPath = s"${prefix}${KeyCredentials.containerNameSilverSchemas}@${KeyCredentials.storageAccountNameSilver}$sufix/semantic"
  private val adlsSilverSemanticPath = s"${prefix}${KeyCredentials.containerNameSilverSemantic}@${KeyCredentials.storageAccountNameSilver}$sufix"
  private val adlsGoldPath = s"${prefix}${KeyCredentials.conatinerNameGoldUseCase}@${KeyCredentials.storageAccountNameGold}$sufix"
  private val pathCheckpoints = s"${prefix}autoloader@${KeyCredentials.storageAccountNameBronze}$sufix/delta/population_data/_checkpoints"

  /**
   * Devuelve la ruta base donde el Auto Loader guarda los checkpoints de streaming.
   *
   * @return la ruta de checkpoints
   */
  def getPathCheckpoints(): String = {
    pathCheckpoints
  }

  /**
   * Method to build the base path of the landing layer of a dataset
   *
   * @param origen
   * @param datasetName
   * @return
   */
  def buildLandingRootPath(origen: String, datasetName: String): String = {
    val patternStaging: String = s"$adlsLandingPath/$origen/$datasetName"
    logger.info(s"Staging pattern: $patternStaging")
    patternStaging
  }

  /**
   * Method to build the base path of the landing layer of a dataset
   *
   * @param origen
   * @param datasetName
   * @return
   */
  def buildLandingRootPathCDC(origen: String, datasetName: String): String = {
    val patternStaging: String = s"$adlsLandingPath/$origen/_checkpoints/$datasetName"
    logger.info(s"Staging pattern: $patternStaging")
    patternStaging
  }

  /**
   * Method to build the path of the landing layer of a dataset
   * @param origen
   * @param datasetName
   * @return
   */
  def buildLandingPath(origen: String, datasetName: String): String = {
    val patternStaging: String = s"$adlsLandingPath/$origen/$datasetName/*"
    logger.info(s"Staging pattern: $patternStaging")
    patternStaging
  }

  /**
   * Method to build the path of the landing layer of a dataset in CDF flow
   *
   * @param origen
   * @param datasetName
   * @return
   */
  def buildLandingPathCDC(origen: String, datasetName: String): String = {
    val patternStaging: String = s"$adlsLandingPath/$origen/_checkpoints/$datasetName/*"
    logger.info(s"Staging pattern: $patternStaging")
    patternStaging
  }

  /**
   * Method to build the path of the raw/source layer of a dataset
   * @param origen
   * @param dataset
   * @return
   */
  def buildRawPath(origen: String, dataset: String): String = {
    val patternBronze: String = s"$adlsRawSourcePath/$origen/$dataset"
    logger.info(s"Bronze pattern: $patternBronze")
    patternBronze
  }

  /**
   * Method to build the path of the raw/clean layer of a dataset
   * @param origen
   * @param dataset
   * @return
   */
  def buildRawCleanPath(origen: String, dataset: String): String = {
    val patternBronze: String = s"$adlsRawCleanPath/$origen/$dataset"
    logger.info(s"Bronze pattern: $patternBronze")
    patternBronze
  }

  /**
   *
   * @param origen
   * @param dataset
   * @return
   */
  def buildSilverDistillationPath(origen: String, dataset: String): String = {
    val patternSilver: String = s"$adlsSilverDistillationPath/$origen/$dataset"
    logger.info(s"Silver pattern: $patternSilver")
    patternSilver
  }

  /**
   *
   * @param origen
   * @param dataset
   * @return
   */
  def buildSilverIntegrationPath(origen: String, dataset: String): String = {
    val patternSilver: String = s"$adlsSilverIntegrationPath/$origen/$dataset"
    logger.info(s"Silver pattern: $patternSilver")
    patternSilver
  }

  /**
   *
   * @param domain_code
   * @param subdomain_code
   * @param subdomain_description
   * @param dataset
   * @return
   */
  def buildSilverSemanticPath(domain_code: String, subdomain_code: String, subdomain_description: String, dataset:String): String = {
    val patternSilverSemantic: String = s"$adlsSilverSemanticPath/${domain_code}_${subdomain_code}_${subdomain_description}/$dataset"
    logger.info(s"Gold pattern by use case: $patternSilverSemantic")
    patternSilverSemantic
  }

  /**
   *
   * @param useCase
   * @return
   */
  //TODO: Add Business Unit
  def buildGoldUseCasePath(useCase: String): String = {
    val patternGoldUseCase: String = s"$adlsGoldPath/${useCase}"
    logger.info(s"Gold pattern by use case: $patternGoldUseCase")
    patternGoldUseCase
  }

  /**
   *
   * @return
   */
  def buildGoldCommonsPath(): String = {
    val patternGoldCommon: String = s"$adlsGoldPath/Common"
    logger.info(s"Gold pattern in Common zone: $patternGoldCommon")
    patternGoldCommon
  }

  /**
   *
   * @param domain
   * @param subdomain
   * @param subdomainDes
   * @return
   */
  def buildDatabaseName(domain: String, subdomain: String, subdomainDes: String): String = {
    val patternDatabase: String = s"${domain}_${subdomain}_${subdomainDes}"
    patternDatabase
  }

  /**
   *
   * @param meta
   * @return
   */
  def buildCustomPath(meta: String*): String = {
    logger.info(s"New path buit: ${meta.map(_.trim).mkString("/")}")
    meta.map(_.trim).mkString("/")
  }

  /**
   * Construye la ruta del CSV de esquema de integración de un dataset.
   *
   * @param origen  source del dataset
   * @param dataset nombre del dataset
   * @return la ruta del fichero de esquema
   */
  def buildSchemasPath(origen: String, dataset: String): String = {
    val patternSchema: String = s"$adlsSilverSchemasPath/$origen/$dataset.csv"
    logger.info(s"Schema pattern: $patternSchema")
    patternSchema
  }

  /**
   * Construye la ruta del YAML de esquema semántico de un dataset.
   *
   * @param domain_code    código del dominio
   * @param subdomain_code código del subdominio
   * @param dataset        nombre del dataset
   * @return la ruta del fichero de esquema semántico
   */
  def buildSchemasSemanticPath(domain_code: String, subdomain_code: String, dataset: String): String = {
    val patternSchema: String = s"$adlsSilverSchemasSemanticPath/${domain_code}/${subdomain_code}/$dataset.yaml"
    logger.info(s"Schema pattern for Semantic: $patternSchema")
    patternSchema
  }
}
