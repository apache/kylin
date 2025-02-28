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

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.exception.KylinException
import org.apache.kylin.metadata.model.{NTableMetadataManager, TableDesc}
import org.apache.kylin.metadata.table.{InternalTableDesc, InternalTableManager}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.catalog.{ClickHouseTableV2, DeltaTableV2}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.scalatest.funsuite.AnyFunSuite

class TestKyinternalCatalog extends AnyFunSuite with SharedSparkSession with LocalMetadata {
  val PROJECT = "default"
  val TABLE_INDENTITY = "DEFAULT.TEST_KYLIN_FACT"
  val DATE_COL = "CAL_DT"
  val ident: Identifier = Identifier.of(Array[String]("default", "DEFAULT"), "TEST_KYLIN_FACT")

  test("test catalog") {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val tManager: NTableMetadataManager = NTableMetadataManager.getInstance(config, PROJECT)
    val internalTableManager: InternalTableManager = InternalTableManager.getInstance(config, PROJECT)
    val table: TableDesc = tManager.getTableDesc(TABLE_INDENTITY)
    val catalog: KyinternalCatalog = new KyinternalCatalog()

    // test load without internal_table
    try {
      catalog.loadTable(ident)
      fail("Expect throw an exception.")
    } catch {
      case _: NoSuchTableException => ;
      case e => fail("Exception type not match", e)
    }

    val internalTable = new InternalTableDesc(table)
    internalTable.setStorageType(InternalTableDesc.StorageType.PARQUET.name())
    internalTable.setTblProperties(new util.HashMap[String, String]())
    internalTable.setLocation(internalTable.generateInternalTableLocation)
    internalTableManager.createInternalTable(internalTable)

    var sparkTable: Table = catalog.loadTable(ident)
    assert(sparkTable.isInstanceOf[ParquetTable])

    internalTableManager.updateInternalTable(TABLE_INDENTITY, (copyForWrite: InternalTableDesc) => {
      copyForWrite.setStorageType(InternalTableDesc.StorageType.GLUTEN.name())
    })
    sparkTable = catalog.loadTable(ident)
    assert(sparkTable.isInstanceOf[ClickHouseTableV2])

    internalTableManager.updateInternalTable(TABLE_INDENTITY, (copyForWrite: InternalTableDesc) => {
      copyForWrite.setStorageType(InternalTableDesc.StorageType.DELTALAKE.name())
    })
    sparkTable = catalog.loadTable(ident)
    assert(sparkTable.isInstanceOf[DeltaTableV2])

    internalTableManager.updateInternalTable(TABLE_INDENTITY, (copyForWrite: InternalTableDesc) => {
      copyForWrite.setStorageType(InternalTableDesc.StorageType.ICEBERG.name())
    })

    try {
      catalog.loadTable(ident)
      fail("Expect throw an exception.")
    } catch {
      case _: KylinException => ;
      case e => fail("Exception type not match", e)
    }
  }
}
