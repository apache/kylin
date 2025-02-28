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

package org.apache.spark.sql.execution.datasources.v2.kyinternal

import java.util

import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.exception.{CommonErrorCode, KylinException}
import org.apache.kylin.metadata.table.{InternalTableDesc, InternalTableManager}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.{ClickHouseTableV2, DeltaTableV2}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, SparderTypeUtil}

/**
 * A custom proxy catalog to support multiple data format/data source for internal table
 * Read kylin metastore and construct table object
 */
class KyinternalCatalog extends TableCatalog
  with SupportsNamespaces
  with Logging {

  val spark = SparkSession.active
  val CLICKHOUSE = InternalTableDesc.StorageType.GLUTEN
  val PARQUET = InternalTableDesc.StorageType.PARQUET
  val DELTA = InternalTableDesc.StorageType.DELTALAKE
  val ICEBERG = InternalTableDesc.StorageType.ICEBERG

  override def name(): String = "INTERNAL_CATALOG"

  override def initialize(s: String, caseInsensitiveStringMap: CaseInsensitiveStringMap): Unit = {
    // Do nothing
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    logInfo(s"listTables: ${namespace.mkString(".")}")
    null
  }

  override def loadTable(ident: Identifier): Table = {
    val project = ident.namespace()(0)
    val kylinConfig = KylinConfig.getInstanceFromEnv
    val tableManager = InternalTableManager.getInstance(kylinConfig, project)
    val tableFullName = ident.namespace()(1) + "." + ident.name()
    val internalDesc = tableManager.getInternalTableDesc(tableFullName)
    if (internalDesc == null) {
      throw NoSuchTableException(tableFullName + " cannot be found.")
    }
    val tblProperties = new CaseInsensitiveStringMap(internalDesc.getTblProperties)
    internalDesc.getStorageType match {
      case PARQUET => ParquetTable(ident.name(), spark, tblProperties,
        Seq(internalDesc.getLocation), Some(getTableSchema(internalDesc)), classOf[ParquetFileFormat])
      case CLICKHOUSE => new ClickHouseTableV2(spark, new Path(internalDesc.getLocation))
      case DELTA => DeltaTableV2(spark, new Path(internalDesc.getLocation))
      case _ => throw new KylinException(CommonErrorCode.INTERNAL_TABLE_UNSUPPORTED_STORAGE_TYPE, "Unsupported storage type")
    }
  }

  def getTableSchema(table: InternalTableDesc): StructType = {
    StructType(table.getColumns.map(col => (col.getName, col.getType)).map {
      pair => StructField(pair._1, SparderTypeUtil.toSparkType(pair._2))
    })
  }

  // no create table implementation
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    null
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    null
  }


  override def dropTable(ident: Identifier): Boolean = {
    true
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException()
  }

  override def listNamespaces(): Array[Array[String]] = {
    Array.empty
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    Array.empty
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    new util.LinkedHashMap[String, String]()
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException()
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException()
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    true
  }
}
