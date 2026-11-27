package com.gestamp.hypatia.databricks

import com.databricks.backend.daemon.dbutils.FileInfo
import com.databricks.dbutils_v1.{DBUtilsHolder, DBUtilsV1}
import com.gestamp.hypatia.adls.KeyCredentials
import com.gestamp.hypatia.exceptions.DatabricksManagerException
import com.gestamp.hypatia.utils.CommonsConstants
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class DatabricksManager extends IDatabricksManager {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val dbutils: DBUtilsV1 = DBUtilsHolder.dbutils

  /**
   * Gets the secret value for the given key from the Databricks secrets scope.
   *
   * @param keyName the name of the key to retrieve the secret value for
   * @return the secret value for the given key
   * @throws DatabricksManagerException if there is an error retrieving the secret value
   */
  override def dbUtilsGetSecret(keyName: String): String = {
    logger.info(s"Reading secret $keyName from Databricks Scope ${CommonsConstants.DB_ADF_SCOPE_NAME_LAB}")
    Try(dbutils.secrets.get(CommonsConstants.DB_ADF_SCOPE_NAME_LAB, keyName)) match {
      case Success(secretValue) => secretValue
      case Failure(e) => throw new DatabricksManagerException(e.getMessage, e)
    }
  }

  /**
   * Mounts the given ADLS path to DBFS for the staging layer.
   *
   * @param configs the configuration map for the ADLS connection
   * @param path    the ADLS path to be mounted to DBFS
   * @return an Either object, with the Left value being a DatabricksManagerException in case of failure,
   *         and the Right value being a string representing the mounted DBFS path in case of success
   * @throws DatabricksManagerException if there is an error mounting the ADLS path to DBFS
   */
  override def dbUtilsFsMountStaging(configs: Map[String, String], path: String): Either[Throwable, String] = {
    logger.info(s"Start Mount ADLS Path to DBFS: Staging Layer: $path")
    Try(mountPathBronze(configs, path, KeyCredentials.containerNameStaging)) match {
      case Success(_) => Right(s"/mnt/$path")
      case Failure(e) =>
        Left(throw new DatabricksManagerException(s"Fallo al hacer el mount del path: $path", e))
    }
  }

  /**
   * Mounts the given ADLS path to DBFS for the bronze layer.
   *
   * @param configs the configuration map for the ADLS connection
   * @param path    the ADLS path to be mounted to DBFS
   * @return an Either object, with the Left value being a DatabricksManagerException in case of failure,
   *         and the Right value being a string representing the mounted DBFS path in case of success
   * @throws DatabricksManagerException if there is an error mounting the ADLS path to DBFS
   */
  override def dbUtilsFsMountBronze(configs: Map[String, String], path: String): Either[Throwable, String] = {
    logger.info(s"Start Mount ADLS Path to DBFS: Bronze Layer: $path")
    Try(mountPathBronze(configs, path, KeyCredentials.containerNameBronze)) match {
      case Success(_) => Right(s"/mnt/$path")
      case Failure(e) =>
        Left(throw new DatabricksManagerException(s"Fallo al hacer el mount del path: $path", e))
    }
  }

  /**
   * Mounts the given ADLS path to DBFS.
   *
   * @param configs   the configuration map for the ADLS connection
   * @param path      the ADLS path to be mounted to DBFS
   * @param container the name of the container in ADLS
   * @return a Try object, with a success value of true if the mount was successful,
   *         and a failure value if there was an error
   */
  override def mountPathBronze(configs: Map[String, String], path: String, container: String): Try[Boolean] = {
    logger.info(s"Mount ADLS Path abfss://${container}@${KeyCredentials.storageAccountNameBronze}.dfs.core.windows.net/ " +
      s"\nto DBFS Bronze Layer: /mnt/$path")
    Try(dbutils.fs.mount(
      source = s"abfss://${container}@${KeyCredentials.storageAccountNameBronze}.dfs.core.windows.net/",
      mountPoint = s"/mnt/$path",
      extraConfigs = configs))
  }

  /**
   * Mounts the given ADLS path to DBFS for the silver layer.
   *
   * @param configs the configuration map for the ADLS connection
   * @param path    the ADLS path to be mounted to DBFS
   * @return a string representing the mounted DBFS path
   * @throws DatabricksManagerException if there is an error mounting the ADLS path to DBFS
   */
  override def dbUtilsFsMountSilver(configs: Map[String, String], path: String): String = {
    logger.info(s"Mount ADLS Path to DBFS: Silver Layer: $path")
    Try(mountPathSilver(configs, path)) match {
      case Success(_) => s"/mnt/$path"
      case Failure(e) => throw new DatabricksManagerException(s"error mounting the path: $path", e)
    }
  }

  /**
   * Mounts the given ADLS path to DBFS.
   *
   * @param configs the configuration map for the ADLS connection
   * @param path    the ADLS path to be mounted to DBFS
   * @return a Try object, with a success value of true if the mount was successful,
   *         and a failure value if there was an error
   */
  override def mountPathSilver(configs: Map[String, String], path: String): Try[Boolean] = {
    logger.info(s"Mount ADLS Path abfss://${KeyCredentials.containerNameSilverDistillation}@${KeyCredentials.storageAccountNameSilver}.dfs.core.windows.net/" +
      s"\nto DBFS: Silver Layer: /mnt/$path")
    Try(dbutils.fs.mount(
      source = s"abfss://${KeyCredentials.containerNameSilverDistillation}@${KeyCredentials.storageAccountNameSilver}.dfs.core.windows.net/",
      mountPoint = s"/mnt/$path",
      extraConfigs = configs))
  }

  /**
   * Mounts the given ADLS path to DBFS for the gold layer.
   *
   * @param configs the configuration map for the ADLS connection
   * @param path    the ADLS path to be mounted to DBFS
   * @return a string representing the mounted DBFS path
   * @throws DatabricksManagerException if there is an error mounting the ADLS path to DBFS
   */
  override def dbUtilsFsMountGold(configs: Map[String, String], path: String): String = {
    logger.info(s"Mount ADLS Path to DBFS: Gold Layer: $path")
    Try(mountPathGold(configs, path)) match {
      case Success(_) => s"/mnt/$path"
      case Failure(e) => throw new DatabricksManagerException(s"error mounting the path: $path", e)

    }
  }

  /**
   * Mounts the given ADLS path to DBFS.
   *
   * @param configs the configuration map for the ADLS connection
   * @param path    the ADLS path to be mounted to DBFS
   * @return a Try object, with a success value of true if the mount was successful,
   *         and a failure value if there was an error
   */
  override def mountPathGold(configs: Map[String, String], path: String): Try[Boolean] = {
    logger.info(s"Mount ADLS Path abfss://${KeyCredentials.conatinerNameGoldUseCase}@${KeyCredentials.storageAccountNameGold}.dfs.core.windows.net/" +
      s"\nto DBFS: Gold Layer: /mnt/$path")
    Try(dbutils.fs.mount(
      source = s"abfss://${KeyCredentials.conatinerNameGoldUseCase}@${KeyCredentials.storageAccountNameGold}.dfs.core.windows.net/",
      mountPoint = s"/mnt/$path",
      extraConfigs = configs))
  }

  /**
   * Copies the files from the source path to the destination path.
   *
   * @param fromPath the source path for the files to be copied
   * @param toPath   the destination path for the copied files
   * @return a string representing the destination path
   * @throws DatabricksManagerException if there is an error copying the files
   */
  override def dbUtilsCopyFiles(fromPath: String, toPath: String): String = {
    logger.info(s"Start Copy files from '$fromPath' to '$toPath")
    Try(fsCopy(fromPath, toPath)) match {
      case Success(_) => toPath
      case Failure(e) => throw new DatabricksManagerException(s"Error coping data from $fromPath to $toPath", e)
    }
  }

  /**
   * Copies a file or directory from one location to another.
   *
   * @param fromPath a String representing the path of the file or directory to be copied
   * @param toPath   a String representing the destination path to which the file or directory should be copied
   * @return a Try[Boolean] object indicating the result of the copy operation. If the computation is successful, the Try
   *         object will contain a Boolean value indicating whether the copy operation was successful or not. If the
   *         computation fails, the Try object will contain an exception indicating the error that occurred.
   */
  override def fsCopy(fromPath: String, toPath: String): Try[Boolean] = {
    logger.info("Run dbUtils.fs.cp activity")
    Try(dbutils.fs.cp(fromPath, toPath, recurse = true))
  }

  /**
   * Unmounts a mounted path in the Databricks File System (DBFS).
   *
   * @param path a String representing the path to be unmounted
   * @return an Either[Throwable, Boolean] object indicating the result of the unmount operation. If the computation is
   *         successful, the Either object will contain a Boolean value indicating the success of the unmount operation. If
   *         the computation fails, the Either object will contain a Throwable object indicating the error that occurred.
   */
  override def dbUtilsFsUnmountPath(path: String): Either[Throwable, Boolean] = {
    logger.info(s"Unmount DBFS path: $path Layer: ")
    Try(dbutils.fs.unmount(path)) match {
      case Success(_) => Right(true)
      case Failure(e) => Left(throw new DatabricksManagerException(s"Error in unmount path: $path", e))
    }
  }

  /**
   * Lists the files and directories in the specified path in the Databricks File System (DBFS).
   *
   * @param path a String representing the path to list the files and directories
   * @return a Seq[FileInfo] object containing information about the files and directories in the specified path. If the
   *         computation fails, a DatabricksManagerException exception will be thrown indicating the error that occurred.
   */
  override def dbUtilsFsLs(path: String): Seq[FileInfo] = {
    logger.info(s"File System 'ls' DBFS path: $path Layer: ")
    Try(dbutils.fs.ls(path)) match {
      case Success(value) => value
      case Failure(e) => throw new DatabricksManagerException(s"Error in dbutils.fs.ls with the path: $path", e)
    }
  }

  /**
   * Delete specified path in the Databricks File System (DBFS).
   *
   * @param path a String representing the path
   * @return Nothing
   */
  override def dbUtilsFsRm(path: String): Unit = {
    logger.info(s"Removing path: $path Layer: ")

    Try(dbutils.fs.ls(path)) match {
      case Success(value) =>
        logger.info(s"The path exists: $path")
        Try(dbutils.fs.rm(path, true)) match {
          case Success(value) => logger.info(s"Path $path was deleted: $value")
          case Failure(e) => throw new DatabricksManagerException(s"Error in dbutils.fs.rm with the path: $path", e)
        }
      case Failure(e) => logger.info(s"The path did not previously exist: $path")
    }


  }

  /**
   * Gets the file format of the last file in the specified path in the Databricks File System (DBFS).
   *
   * @param path a String representing the path to get the file format
   * @return a String containing the file format of the last file in the specified path. If the computation fails, a
   *         DatabricksManagerException exception will be thrown indicating the error that occurred.
   */
  override def getFileFormat(path: String): String = {
    logger.info(s"Get File Format for path: $path")

    val fileInfo: Seq[FileInfo] = dbUtilsFsLs(path)
    if (fileInfo.size >= 1) {
      Try(fileInfo.last.path.split('.').last) match {
        case Success(value) =>
          logger.info(s"El formato del fichero es: $value")
          value
        case Failure(e) => throw new DatabricksManagerException(s"Error trying to get the file format in the path: $path", e)
      }
    } else {
      throw new DatabricksManagerException(s"There are not files in the path: $path")
    }
  }
}
