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

package org.apache.kylin.engine.spark.job

import io.kyligence.kap.engine.spark.stats.analyzer.TableAnalyzerJob
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.NSparkCubingEngine
import org.apache.kylin.engine.spark.builder.CreateFlatTable
import org.apache.kylin.engine.spark.source.SparkSqlUtil
import org.apache.kylin.engine.spark.utils.SparkConfHelper
import org.apache.kylin.metadata.model.TableDesc
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.kylin.source.SourceFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrameEnhancement._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

class TableAnalysisJob(tableDesc: TableDesc,
                       project: String,
                       rowCount: Long,
                       ss: SparkSession,
                       jobId: String) extends Serializable with Logging {

  // it's a experimental value recommended by Spark,
  // which used for controlling the TableSampling tasks' count.
  val taskFactor = 4

  def analyzeTable(): Array[Row] = {
    val sparkConf = ss.sparkContext.getConf

    val instances = sparkConf.get(SparkConfHelper.EXECUTOR_INSTANCES, "1").toInt
    val cores = sparkConf.get(SparkConfHelper.EXECUTOR_CORES, "1").toInt
    val numPartitions = instances * cores
    val rowsTakenInEachPartition = rowCount / numPartitions
    val params = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv)
      .getProject(tableDesc.getProject).getLegalOverrideKylinProps
    params.put("sampleRowCount", String.valueOf(rowCount))
    val dataFrame = SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, params)
      .coalesce(numPartitions)

    calculateViewMetasIfNeeded(tableDesc.getBackTickIdentity)

    val dat = dataFrame.localLimit(rowsTakenInEachPartition)
    val sampledDataset = CreateFlatTable.changeSchemaToAliasDotName(dat, tableDesc.getBackTickIdentity)
    // todo: use sample data to estimate total info
    // calculate the stats info
    val statsMetrics = buildStatsMetric(sampledDataset)
    val aggData = sampledDataset.agg(count(lit(1)), statsMetrics: _*).collect()

    aggData ++ sampledDataset.limit(10).collect()
  }

  def calculateViewMetasIfNeeded(tableName: String): Unit = {
    if (ss.conf.get("spark.sql.catalogImplementation") == "hive") {
      if (ss.catalog.tableExists(tableName)) {
        val sparkTable = ss.catalog.getTable(tableName)
        if (sparkTable.tableType == CatalogTableType.VIEW.name) {
          val tables = SparkSqlUtil.getViewOrignalTables(tableName, ss)
          if (tables.asScala.size > 1) {
            tables.asScala.foreach(t => {
              var oriTable = t
              if (!t.contains(".")) {
                oriTable = sparkTable.database + "." + t
              }
              val rowCnt = ss.table(oriTable).count()
              logInfo(s"Table $oriTable true number of rows is $rowCnt")
              TableMetaManager.putTableMeta(t, 0L, rowCnt)
            })
            logInfo(s"Table type ${sparkTable.tableType}, orignal table num is ${tables.asScala.size}")
          }
        }
      }
    }
  }

  def buildStatsMetric(sourceTable: Dataset[Row]): List[Column] = {
    sourceTable.schema.fieldNames.flatMap(
      name =>
        Seq(TableAnalyzerJob.TABLE_STATS_METRICS.toArray(): _*).map {
          case "COUNT" =>
            count(col(name))
          case "COUNT_DISTINCT" =>
            approx_count_distinct(col(name))
          case "MAX" =>
            max(col(name))
          case "MIN" =>
            min(col(name))
          case _ =>
            throw new IllegalArgumentException(
              s"""Unsupported metric in TableSampling """)
        }
    ).toList
  }

}
