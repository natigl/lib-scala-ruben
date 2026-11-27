package com.gestamp.hypatia.adls

import com.gestamp.hypatia.exceptions.ADLS2ManagerException

/**
 * Pure helpers (no Spark) that decide how a DataFrame must be written to a Delta table.
 *
 * A table that already exists is written by name (no `path` option): Databricks rejects any path-based
 * access to a table with row filter or column mask (Security Mesh ABAC policies), and writing by name keeps
 * the table's tags, row filters, grants, history and LOCATION. A table that does not exist yet is created
 * with the `path` option, so it is born as an EXTERNAL table in the requested location.
 */
object WriteTargetResolver {

  private val SchemeSeparator: String = "://"

  /**
   * Decides the write target of a `saveAsTable`.
   *
   * @param tableName        the full name of the target table (catalog.schema.table), used in the error messages
   * @param tableExists      true if the target table already exists in the catalog
   * @param existingLocation the LOCATION of the existing table (DESCRIBE DETAIL), if known
   * @param path             the path requested by the caller
   * @return Right(Some(path)) to write with the `path` option (the table does not exist yet),
   *         Right(None) to write by table name (the table exists in the requested path), or
   *         Left(ADLS2ManagerException) if the table exists and its location is unknown or differs from `path`
   */
  private[hypatia] def resolveWriteTarget(tableName: String, tableExists: Boolean, existingLocation: Option[String],
                                          path: String): Either[ADLS2ManagerException, Option[String]] = {
    if (!tableExists) {
      Right(Some(path))
    } else {
      existingLocation match {
        case Some(location) if samePath(location, path) => Right(None)
        case Some(location) => Left(new ADLS2ManagerException(
          s"Spark Writer => The location of the existing table $tableName does not match the requested path, " +
            s"the data is not written. Existing location = $location, requested path = $path"))
        case None => Left(new ADLS2ManagerException(
          s"Spark Writer => The table $tableName already exists but its location is unknown, the data is not written. " +
            s"Requested path = $path"))
      }
    }
  }

  /**
   * Compares two storage paths after normalizing them with [[normalizePath]].
   *
   * @param a first path
   * @param b second path
   * @return true if both paths point to the same location
   */
  private[hypatia] def samePath(a: String, b: String): Boolean = normalizePath(a) == normalizePath(b)

  /**
   * Normalizes a storage path to compare it: trims blanks, lowercases the scheme and the authority
   * (`container@account.dfs.core.windows.net`, case-insensitive in Azure), collapses repeated slashes after
   * the scheme and removes the trailing slashes. The rest of the path keeps its case.
   *
   * @param path the path to normalize (null is treated as empty)
   * @return the normalized path
   */
  private[hypatia] def normalizePath(path: String): String = {
    val trimmed = Option(path).map(_.trim).getOrElse("")
    val schemeIdx = trimmed.indexOf(SchemeSeparator)

    if (schemeIdx < 0) {
      collapseSlashes(trimmed)
    } else {
      val scheme = trimmed.substring(0, schemeIdx).toLowerCase
      val rest = trimmed.substring(schemeIdx + SchemeSeparator.length).replaceAll("^/+", "")
      val authorityEnd = rest.indexOf('/')
      val (authority, relative) =
        if (authorityEnd < 0) (rest, "") else (rest.substring(0, authorityEnd), rest.substring(authorityEnd))

      s"$scheme$SchemeSeparator${authority.toLowerCase}${collapseSlashes(relative)}"
    }
  }

  private def collapseSlashes(path: String): String = path.replaceAll("/{2,}", "/").replaceAll("/+$", "")
}
