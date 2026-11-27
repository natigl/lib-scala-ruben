package com.gestamp.hypatia

import com.gestamp.hypatia.utils.tag_propagation.{SystemTableAccessor, TableRef}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Test implementation of SystemTableAccessor that uses mock data from temporary views.
 */
class MockSystemTableAccessor(spark: SparkSession) extends SystemTableAccessor {

  override def getColumnTags(table: TableRef, columnName: String): DataFrame = {
    spark.sql(s"""
      SELECT tag_name, tag_value
      FROM mock_column_tags
      WHERE catalog_name = '${table.catalog}'
        AND schema_name = '${table.schema}'
        AND table_name = '${table.table}'
        AND column_name = '$columnName'
    """)
  }

  override def getAllColumnTags(table: TableRef): DataFrame = {
    spark.sql(s"""
      SELECT column_name, tag_name, tag_value
      FROM mock_column_tags
      WHERE catalog_name = '${table.catalog}'
        AND schema_name = '${table.schema}'
        AND table_name = '${table.table}'
    """)
  }

  override def getTableColumns(table: TableRef): DataFrame = {
    spark.sql(s"""
      SELECT column_name
      FROM mock_columns
      WHERE table_catalog = '${table.catalog}'
        AND table_schema = '${table.schema}'
        AND table_name = '${table.table}'
      ORDER BY ordinal_position
    """)
  }

  override def getColumnLineage(targetTable: TableRef, columnName: String, daysBack: Int): DataFrame = {
    spark.sql(s"""
      SELECT DISTINCT
        source_table_catalog,
        source_table_schema,
        source_table_name,
        source_column_name
      FROM mock_column_lineage
      WHERE target_table_full_name = '${targetTable.full}'
        AND target_column_name = '$columnName'
        AND event_date >= current_date() - INTERVAL $daysBack DAYS
        AND source_table_full_name IS NOT NULL
        AND source_column_name IS NOT NULL
    """)
  }

  override def getRecursiveColumnLineage(targetTable: TableRef, columnName: String,
                                          targetLayerPattern: String, maxDepth: Int, daysBack: Int): DataFrame = {
    val fullTableName = targetTable.full
    // Simplified version for testing - just returns direct lineage filtered by pattern
    // In a real test, you could implement the recursive logic or use pre-computed test data
    spark.sql(s"""
      SELECT DISTINCT
        source_table_catalog,
        source_table_schema,
        source_table_name,
        source_column_name,
        1 AS depth
      FROM mock_column_lineage
      WHERE target_table_full_name = '$fullTableName'
        AND target_column_name = '$columnName'
        AND event_date >= current_date() - INTERVAL $daysBack DAYS
        AND source_table_full_name IS NOT NULL
        AND source_column_name IS NOT NULL
        AND (LOWER(source_table_catalog) LIKE '%${targetLayerPattern.toLowerCase}%'
          OR LOWER(source_table_schema) LIKE '%${targetLayerPattern.toLowerCase}%'
          OR LOWER(source_table_name) LIKE '%${targetLayerPattern.toLowerCase}%')
      ORDER BY depth
    """)
  }
}
