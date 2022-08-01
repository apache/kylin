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

package org.apache.spark.sql.hive

import org.apache.kylin.engine.spark.job.TableMetaManager
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types.{IntegerType, StructType}

import java.net.URI

class ReplaceLocationRuleTest extends SparderBaseFunSuite with SharedSparkSession{
  test("ReplaceLocationRuleTest") {

    TableMetaManager.putTableMeta("db1.tbl", 0, 0)
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.apply(Option(new URI("hdfs://hacluster")), Option.empty, Option.empty, Option.empty, false, Map.empty),
      owner = null,
      provider = Some("hive"),
      schema = new StructType().add("col1", "int"),
      stats = Some(CatalogStatistics(BigInt(0), None))
    )

    val relation = new HiveTableRelation(table, table.dataSchema.asNullable.toAttributes, table.partitionSchema.asNullable.toAttributes)
    val afterRulePlan = ReplaceLocationRule.apply(spark).apply(relation)
    assert(afterRulePlan.asInstanceOf[HiveTableRelation].tableMeta.storage.toString() == "Storage(Location: hdfs://hacluster)")

    spark.sessionState.conf.setLocalProperty("spark.sql.hive.specific.fs.location", "hdfs://writecluster")
    val afterRulePlan1 = ReplaceLocationRule.apply(spark).apply(relation)
    assert(afterRulePlan1.asInstanceOf[HiveTableRelation].tableMeta.storage.toString() == "Storage(Location: hdfs://writecluster)")
  }

  test("ReplaceLocationRuleTestForOtherCase") {
    TableMetaManager.putTableMeta("db1.tbl", 0, 0)
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.apply(Option(new URI("hdfs://hacluster")), Option.empty, Option.empty, Option.empty, false, Map.empty),
      owner = null,
      provider = Some("parquet"),
      schema = new StructType().add("col1", "int"),
      stats = Some(CatalogStatistics(BigInt(0), None))
    )

    val relation = new HiveTableRelation(table, table.dataSchema.asNullable.toAttributes, table.partitionSchema.asNullable.toAttributes)
    val afterRulePlan = ReplaceLocationRule.apply(spark).apply(relation)
    assert(afterRulePlan.asInstanceOf[HiveTableRelation].tableMeta.storage.toString() == "Storage(Location: hdfs://hacluster)")

    val table1 = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      owner = null,
      provider = Some("hive"),
      schema = new StructType().add("col1", "int"),
      stats = Some(CatalogStatistics(BigInt(0), None))
    )

    val relation1 = new HiveTableRelation(table1, table1.dataSchema.asNullable.toAttributes, table1.partitionSchema.asNullable.toAttributes)
    val afterRulePlan1 = ReplaceLocationRule.apply(spark).apply(relation1)
    assert(afterRulePlan1.asInstanceOf[HiveTableRelation].tableMeta.storage.toString() == "Storage()")

    try {
      ReplaceLocationRule.apply(spark).apply(null)
    } catch {
      case _: Exception =>
    }

    val localRelation = LocalRelation(
      Seq(AttributeReference("a", IntegerType, nullable = true)()), isStreaming = false)
    try {
      ReplaceLocationRule.apply(spark).apply(localRelation)
    } catch {
      case _: Exception =>
    }

  }
}
