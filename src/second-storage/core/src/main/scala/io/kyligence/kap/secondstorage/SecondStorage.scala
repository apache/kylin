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

package io.kyligence.kap.secondstorage

import io.kyligence.kap.secondstorage.enums.LockTypeEnum
import io.kyligence.kap.secondstorage.metadata._
import org.apache.kylin.common.{ForceToTieredStorage, KylinConfig, QueryContext}
import org.apache.kylin.engine.spark.utils.JavaOptionals._
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataflow}
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.SQLException
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object SecondStorage extends LogEx {

  private[secondstorage] def load(pluginClassName: String): SecondStoragePlugin = {
    try {
      // scalastyle:off classforname
      val pluginClass =
        Option.apply(Class.forName(pluginClassName, true, Thread.currentThread.getContextClassLoader))
      // scalastyle:on classforname

      pluginClass
        .flatMap { p =>
          if (!classOf[SecondStoragePlugin].isAssignableFrom(p)) {
            logWarning(s"SecondStoragePlugin plugin class not found: $pluginClassName is not defined")
            Option(null)
          } else {
            Some(p.getDeclaredConstructor().newInstance().asInstanceOf[SecondStoragePlugin])
          }
        }
        .orNull
    } catch {
      case e: ClassNotFoundException =>
        logWarning(s"ClassNotFoundException", e)
        null
    }
  }

  private var secondStoragePlugin: SecondStoragePlugin = _

  lazy val configLoader: SecondStorageConfigLoader = {
    if (secondStoragePlugin == null) {
      throw new RuntimeException("second storage plugin is null")
    }
    secondStoragePlugin.getConfigLoader
  }

  def init(force: Boolean): Unit = {
    if (force || secondStoragePlugin == null) {
      secondStoragePlugin = Option(KylinConfig.getInstanceFromEnv.getSecondStorage).map(load).orNull
    }
  }

  def enabled: Boolean = secondStoragePlugin != null && secondStoragePlugin.ready()

  def tableFlowManager(config: KylinConfig, project: String): Manager[TableFlow] =
    secondStoragePlugin.tableFlowManager(config, project)

  def tableFlowManager(dataflow: NDataflow): Manager[TableFlow] =
    tableFlowManager(dataflow.getConfig, dataflow.getProject)

  def tablePlanManager(config: KylinConfig, project: String): Manager[TablePlan] =
    secondStoragePlugin.tablePlanManager(config, project)

  def nodeGroupManager(config: KylinConfig, project: String): Manager[NodeGroup] =
    secondStoragePlugin.nodeGroupManager(config, project)

  private def queryCatalog() = Option.apply(secondStoragePlugin.queryCatalog())

  private def forcedToTieredStorageHandler() = {
    var forcedToTieredStorage : ForceToTieredStorage = QueryContext.current.getForcedToTieredStorage
    if (forcedToTieredStorage == null) {
      forcedToTieredStorage = ForceToTieredStorage.CH_FAIL_TO_DFS
    }
    if (forcedToTieredStorage != ForceToTieredStorage.CH_FAIL_TO_DFS) {
      throw new SQLException(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE)
    }
  }
  def trySecondStorage(
                        sparkSession: SparkSession,
                        dataflow: NDataflow,
                        layout: LayoutEntity,
                        pruningInfo: String): Option[DataFrame] = {
    if (!SecondStorageUtil.isModelEnable(dataflow.getProject, dataflow.getModel.getId)) {
      return None
    }

    if (LockTypeEnum.locked(LockTypeEnum.QUERY.name(), SecondStorageUtil.getProjectLocks(dataflow.getProject))) {
      log.info("project={} has 'QUERY' lock, can not hit clickhouse query.", dataflow.getProject)
      return Option.empty
    }

    var result = Option.empty[DataFrame]
    // Only support table index
    val enableSSForThisQuery = enabled && layout.getIndex.isTableIndex
    val allSegIds = pruningInfo.split(",").map(s => s.split(":")(0)).toSet.asJava
    while (enableSSForThisQuery && result.isEmpty && QueryContext.current().isRetrySecondStorage) {
      val tableData = Option.apply(enableSSForThisQuery)
        .filter(_ == true)
        .flatMap(_ =>
          tableFlowManager(dataflow)
            .get(dataflow.getUuid)
            .flatMap(f => f.getEntity(layout))
            .toOption)
        .filter { tableData =>
          tableData.containSegments(allSegIds)
        }
      if (tableData.isEmpty) {
        QueryContext.current().setRetrySecondStorage(false)
        logInfo("No table data found.")
      }
      result = tableData.flatMap(tableData =>
        tryCreateDataFrame(Some(tableData), sparkSession, dataflow, allSegIds)
      )
    }
    if (result.isDefined) {
      QueryContext.current().setLastFailed(false)
      QueryContext.current().getSecondStorageUsageMap.put(layout.getId, true)
    }
    if (enableSSForThisQuery && result.isEmpty) {
      forcedToTieredStorageHandler()
    }

    result
  }

  private def tryCreateDataFrame(tableData: Option[TableData], sparkSession: SparkSession,
                                 dataflow: NDataflow, allSegIds: java.util.Set[String]) = {
    try {
      var project = dataflow.getProject
      for {
        shardJDBCURLs <- tableData.map(_.getShardJDBCURLs(project, allSegIds))
        database <- tableData.map(_.getDatabase)
        table <- tableData.map(_.getTable)
        catalog <- queryCatalog()
        numPartition <- tableData.map(_.getSchemaURLSize)
      } yield {
        sparkSession.read
          .option(ShardOptions.SHARD_URLS, shardJDBCURLs)
          .option(ShardOptions.PUSHDOWN_AGGREGATE, true)
          .option(ShardOptions.PUSHDOWN_LIMIT, true)
          .option(ShardOptions.PUSHDOWN_OFFSET, true)
          .option(ShardOptions.PUSHDOWN_NUM_PARTITIONS, numPartition)
          .table(s"$catalog.$database.$table")
      }
    } catch {
      case NonFatal(e) =>
        logDebug("Failed to use second storage table-index", e)
        forcedToTieredStorageHandler()
        QueryContext.current().setLastFailed(true);
        None
    }
  }
}
