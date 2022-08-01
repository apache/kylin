/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.kylin.external

import java.util.Locale

import org.apache.kylin.externalCatalog.api.catalog.{FieldSchema, IExternalCatalog, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.kylin.external.HasKeExternal.getSparkSQLDataType
import org.apache.spark.sql.types.{StructField, StructType}


class KylinExternalCatalog(
                            conf: SparkConf = new SparkConf,
                            hadoopConfig: Configuration = new Configuration,
                            val keExternalCatalog: IExternalCatalog)
  extends InMemoryCatalog(conf, hadoopConfig) with Logging with HasKeExternal {

  override def createDatabase(
                               dbDefinition: CatalogDatabase,
                               ignoreIfExists: Boolean): Unit = {
    super.createDatabase(dbDefinition, ignoreIfExists)
  }

  override def getDatabase(db: String): CatalogDatabase = {
    getExternalDatabase(db).getOrElse(super.getDatabase(db))
  }
  override def databaseExists(db: String): Boolean = {
    databaseExistsInExternal(db) || super.databaseExists(db)
  }

  override def listDatabases(): Seq[String] = {
    listExternalDatabases().union(super.listDatabases().map(_.toUpperCase(Locale.ROOT))).distinct.sorted
  }

  override def tableExists(db: String, table: String): Boolean = {
    tableExistsInExternal(db, table) || super.tableExists(db, table)
  }

  override def listPartitions(db: String,
                              table: String,
                              partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    listPartitionsInExternal(db, table)
  }

  override def listTables(db: String): Seq[String] = {
    if (databaseExistsInExternal(db)) {
      listExternalTables(db, ".*")
    } else {
      super.listTables(db)
    }
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    listExternalTables(db, pattern)
      .union {
        if (super.databaseExists(db)) {
          StringUtils.filterPattern(super.listTables(db), pattern).map(_.toUpperCase(Locale.ROOT))
        } else {
          Nil
        }
      }
      .distinct
      .sorted
  }

  override def getTable(db: String, table: String): CatalogTable = {
    getExternalTable(db, table).getOrElse(super.getTable(db, table))
  }
}

object KylinExternalCatalog {

  def fromExternalFormat(format: String): String = {
    Enum.valueOf(classOf[Table.Format], format) match {
      case Table.Format.JSON => "org.apache.spark.sql.json"
      case Table.Format.CSV => "com.databricks.spark.csv"
      case Table.Format.PARQUET => "org.apache.spark.sql.parquet"
      case Table.Format.ORC => "org.apache.spark.sql.hive.orc"
      case _ => throw new UnsupportedOperationException(format)
    }
  }

  def toExternalFormate(provider: String): Table.Format = {
    provider.toLowerCase(Locale.ROOT) match {
      case "org.apache.spark.sql.json" | "json" => Table.Format.JSON
      case "com.databricks.spark.csv" | "csv" => Table.Format.CSV
      case "org.apache.spark.sql.parquet" | "parquet" => Table.Format.PARQUET
      case "org.apache.spark.sql.hive.orc" | "orc" => Table.Format.ORC
      case _ => throw new UnsupportedOperationException(provider)
    }
  }

  /** Converts the native StructField to Hive's FieldSchema. */
  def toExternalColumn(c: StructField): FieldSchema = {
    val typeString = if (c.metadata.contains("__CHAR_VARCHAR_TYPE_STRING")) {
      c.metadata.getString("__CHAR_VARCHAR_TYPE_STRING")
    } else {
      c.dataType.catalogString
    }
    new FieldSchema(c.name, typeString, c.getComment().orNull)
  }

  def verifyColumnDataType(schema: StructType): Unit = {
    schema.foreach(col => getSparkSQLDataType(toExternalColumn(col)))
  }

  def toExternalType(tableType: CatalogTableType): Table.Type = {
    tableType match {
      case CatalogTableType.VIEW => Table.Type.VIEW
      case _ => Table.Type.EXTERNAL_TABLE
    }
  }

  def fromExternalType(tableType: String): CatalogTableType = {
    Enum.valueOf(classOf[Table.Type], tableType) match {
      case Table.Type.VIEW => CatalogTableType.VIEW
      case Table.Type.EXTERNAL_TABLE => CatalogTableType.EXTERNAL
      case _ => throw new UnsupportedOperationException(tableType)
    }
  }
}
