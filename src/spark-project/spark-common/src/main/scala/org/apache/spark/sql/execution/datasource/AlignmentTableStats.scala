/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasource

import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.kylin.engine.spark.job.TableMetaManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

case class AlignmentTableStats(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case htr: HiveTableRelation
      if DDLUtils.isHiveTable(htr.tableMeta) =>
      val tableIdentity = htr.tableMeta.qualifiedName
      val storedStats = TableMetaManager.getTableMeta(tableIdentity)
      if (storedStats.isDefined) {
        val originSizeInBytes = htr.tableMeta.stats match {
          case Some(stats) =>
            stats.sizeInBytes
          case None => BigInt(10000000000000000L)
        }
        val alignedStats = CatalogStatistics(sizeInBytes = originSizeInBytes, rowCount = storedStats.get.rowCount)
        logInfo(s"Aligned hive table statistics: [$tableIdentity, $alignedStats]")
        val withStats = htr.tableMeta.copy(stats = Some(alignedStats))
        htr.copy(tableMeta = withStats)
      } else {
        logInfo(s"No aligned statistics found of hive table $tableIdentity")
        htr
      }
    case lr @ LogicalRelation(_, _, Some(catalogTable), _) =>
      val tableIdentity = catalogTable.qualifiedName
      val storedStats = TableMetaManager.getTableMeta(tableIdentity)
      if (storedStats.isDefined) {
        val originSizeInBytes = catalogTable.stats match {
          case Some(stats) =>
            stats.sizeInBytes
          case None => BigInt(10000000000000000L)
        }
        val alignedStats = CatalogStatistics(sizeInBytes = originSizeInBytes, rowCount = storedStats.get.rowCount)
        logInfo(s"Aligned catalog table statistics: [$tableIdentity, $alignedStats]")
        val withStats = lr.catalogTable.map(_.copy(stats = Some(alignedStats)))
        lr.copy(catalogTable = withStats)
      } else {
        logInfo(s"No aligned statistics found of catalog table $tableIdentity")
        lr
      }
  }
}
