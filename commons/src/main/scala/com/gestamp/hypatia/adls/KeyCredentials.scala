package com.gestamp.hypatia.adls

import com.gestamp.hypatia.databricks.DatabricksManager

object KeyCredentials {

  val clientId: String = "hypatia-dataset-manager-id"
  val clientSecret: String = "hypatia-dataset-manager-secret"
  val tenantId: String = "gestamp-tenantId"

  //val stagingI40: String = "stagingi40-url"
  val bronzeURL: String = "bronze-url"
  val silverURL: String = "silver-url"
  val goldURL: String = "gold-url"

  val containerNameIotI40: String = "staging"
  val containerNameBronze: String = "raw"
  val containerNameStaging: String = "landing"
  val containerNameSilverDistillation: String = "distillation"
  val containerNameSilverIntegration: String = "integration"
  val containerNameSilverSemantic: String = "semantic"
  val containerNameGoldCommons: String = "commons"
  val conatinerNameGoldUseCase: String = "application"
  val containerNameSilverSchemas: String = "schemas"

  //val azureTableConn: String = "azure-tables-connection-string"
  val PatternStName = """st.*neu""".r

  val databricksManager = new DatabricksManager()
  //val storageAccountNameIoti40: String = PatternStName.findFirstIn(DatabricksManager.dbUtilsGetSecret(stagingI40)).get
  val storageAccountNameBronze: String = PatternStName.findFirstIn(databricksManager.dbUtilsGetSecret(bronzeURL)).get
  val storageAccountNameSilver: String = PatternStName.findFirstIn(databricksManager.dbUtilsGetSecret(silverURL)).get
  val storageAccountNameGold: String = PatternStName.findFirstIn(databricksManager.dbUtilsGetSecret(goldURL)).get
  //val azureTablesConnectionString: String = DatabricksManager.dbUtilsGetSecret(azureTableConn)

  override def toString = s"KeyCredentials(clientId=$clientId, clientSecret=$clientSecret, tenantId=$tenantId)"

}
