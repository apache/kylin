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

package org.apache.spark.sql.execution

import org.apache.kylin.engine.spark.job.TableMetaManager
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.datasource.AlignmentTableStats
import org.apache.spark.sql.types.StructType

class AlignmentTableStatsSuite extends SparderBaseFunSuite with SharedSparkSession {
  test("AlignmentTableStatsSuite") {

    TableMetaManager.putTableMeta("db1.tbl", 0, 11111)
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      owner = null,
      provider = Some("hive"),
      schema = new StructType().add("col1", "int"),
      stats = Some(CatalogStatistics(BigInt(0), None))
    )

    val relation = new HiveTableRelation(table, table.dataSchema.asNullable.toAttributes, table.partitionSchema.asNullable.toAttributes)
    val afterRulePlan = AlignmentTableStats.apply(spark).apply(relation)
    assert(afterRulePlan.asInstanceOf[HiveTableRelation].tableMeta.stats.get.rowCount == Some(11111))
  }
}


