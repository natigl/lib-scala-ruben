package com.gestamp.hypatia.databricks


import org.scalatest.FunSuite
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import com.databricks.dbutils_v1.DBUtilsV1


class DatabricksManagerSuite extends FunSuite {

  /*test("dbUtilsGetSecret returns secret value") {
    val dbutilsMockV1 = mock(classOf[DBUtilsV1])
    when(dbutilsMockV1.secrets.get(any[String], any[String])).thenReturn("mySecretValue")

    val manager = new DatabricksManager() {
      override val dbutils: DBUtilsV1 = dbutilsMockV1
    }

    val secretValue = manager.dbUtilsGetSecret("myKey")
    assert(secretValue === "mySecretValue")
  }*/

}
