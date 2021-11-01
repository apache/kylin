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
package org.apache.kylin.query.runtime.plans

import org.apache.kylin.shaded.com.google.common.cache.{Cache, CacheBuilder}
import org.apache.kylin.shaded.com.google.common.collect.Lists
import org.apache.calcite.linq4j.{Enumerable, Linq4j}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.kylin.common.debug.BackdoorToggles
import org.apache.kylin.common.exceptions.KylinTimeoutException
import org.apache.kylin.common.{KylinConfig, QueryContext, QueryContextFacade}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.project.ProjectManager
import org.apache.kylin.query.runtime.plans.ResultType.ResultType
import org.apache.kylin.query.util.SparkJobTrace
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparderContext}
import org.apache.spark.sql.hive.utils.QueryMetricUtils
import org.apache.spark.sql.utils.SparkTypeUtil
import org.apache.spark.utils.SparderUtils

import scala.collection.JavaConverters._

// scalastyle:off
object ResultType extends Enumeration {
  type ResultType = Value
  val ASYNC, NORMAL, SCALA = Value
}

object ResultPlan extends Logging {

  def collectEnumerable(
    df: DataFrame,
    rowType: RelDataType): Enumerable[Array[Any]] = {
    val rowsItr: Array[Array[Any]] = collectInternal(df, rowType)
    Linq4j.asEnumerable(rowsItr.array)
  }

  def collectScalarEnumerable(
    df: DataFrame,
    rowType: RelDataType): Enumerable[Any] = {
    val rowsItr: Array[Array[Any]] = collectInternal(df, rowType)
    val x = rowsItr.toIterable.map(a => a.apply(0)).asJava
    Linq4j.asEnumerable(x)
  }

  private def collectInternal(
    df: DataFrame,
    rowType: RelDataType): Array[Array[Any]] = {
    val resultTypes = rowType.getFieldList.asScala
    val jobGroup = Thread.currentThread().getName
    val sparkContext = SparderContext.getSparkSession.sparkContext
    val projectName = QueryContextFacade.current().getProject
    var kylinConfig = KylinConfig.getInstanceFromEnv
    if (projectName != null) {
      kylinConfig = ProjectManager.getInstance(kylinConfig).getProject(projectName).getConfig
    }
    var pool = "heavy_tasks"
    val sparderTotalCores = SparderUtils.getTotalCore(df.sparkSession.sparkContext.getConf)
    // this value of partition num only effects when querying from snapshot tables
    val partitionsNum =
      if (kylinConfig.getSparkSqlShufflePartitions != -1) {
        kylinConfig.getSparkSqlShufflePartitions
      } else {
        sparderTotalCores
      }

    if (QueryContextFacade.current().isHighPriorityQuery) {
      pool = "vip_tasks"
    } else if (partitionsNum <= sparderTotalCores) {
      pool = "lightweight_tasks"
    }

    if (kylinConfig.getProjectQuerySparkPool != null) {
      pool = kylinConfig.getProjectQuerySparkPool
    }
    if (BackdoorToggles.getDebugToggleSparkPool != null) {
      pool = BackdoorToggles.getDebugToggleSparkPool
    }

    // set priority
    sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    QueryContextFacade.current().setSparkPool(pool)
    val queryId = QueryContextFacade.current().getQueryId
    sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_ID_KEY, queryId)
    df.sparkSession.conf.set("spark.sql.shuffle.partitions", partitionsNum.toString)
    logInfo(s"Set partition to $partitionsNum")
    QueryContextFacade.current().setDataset(df)

    sparkContext.setJobGroup(jobGroup,
      "Query Id: " + QueryContextFacade.current().getQueryId,
      interruptOnCancel = true)
    val currentTrace = QueryContextFacade.current().getQueryTrace
    currentTrace.endLastSpan()
    val jobTrace = new SparkJobTrace(jobGroup, currentTrace, sparkContext)
    try {
      val rows = df.collect()
      jobTrace.jobFinished()
      val (scanRows, scanFiles, metadataTime, scanTime, scanBytes) = QueryMetricUtils.collectScanMetrics(df.queryExecution.executedPlan)
      QueryContextFacade.current().addAndGetScannedRows(scanRows.asScala.map(Long2long(_)).sum)
      QueryContextFacade.current().addAndGetScanFiles(scanFiles.asScala.map(Long2long(_)).sum)
      QueryContextFacade.current().addAndGetScannedBytes(scanBytes.asScala.map(Long2long(_)).sum)
      QueryContextFacade.current().addAndGetMetadataTime(metadataTime.asScala.map(Long2long(_)).sum)
      QueryContextFacade.current().addAndGetScanTime(scanTime.asScala.map(Long2long(_)).sum)
      val dt = rows.map { row =>
        var rowIndex = 0
        row.toSeq.map { cell => {
          var vale = cell
          val rType = resultTypes.apply(rowIndex).getType
          val value = SparkTypeUtil.convertStringToValue(vale,
            rType,
            toCalcite = true)
          rowIndex = rowIndex + 1
          value
        }
        }.toArray
      }
      dt
    } catch {
      case e: InterruptedException =>
        //        QueryContextFacade.current().setTimeout(true)
        sparkContext.cancelJobGroup(jobGroup)
        logInfo(
          s"Query timeouts after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s",
          e)
        throw new KylinTimeoutException(
          s"Query timeout after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s");
    } finally {
      //      QueryContextFacade.current().setExecutionID(QueryToExecutionIDCache.getQueryExecutionID(queryId))
    }
  }

  /**
   * use to check acl  or other
   *
   * @param df   finally df
   * @param body resultFunc
   * @tparam U
   * @return
   */
  def withScope[U](df: DataFrame)(body: => U): U = {
    HadoopUtil.setCurrentConfiguration(df.sparkSession.sparkContext.hadoopConfiguration)
    val r = body
    // remember clear local properties.
    df.sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", null)
    SparderContext.setDF(df)
    TableScanPlan.cacheDf.get().clear()
    HadoopUtil.setCurrentConfiguration(null)
    r
  }

  def getResult(df: DataFrame, rowType: RelDataType, resultType: ResultType)
  : Either[Enumerable[Array[Any]], Enumerable[Any]] = withScope(df) {
    val result: Either[Enumerable[Array[Any]], Enumerable[Any]] =
      resultType match {
        case ResultType.NORMAL =>
          if (SparderContext.needCompute()) {
            Left(ResultPlan.collectEnumerable(df, rowType))
          } else {
            Left(Linq4j.asEnumerable(Array.empty[Array[Any]]))
          }
        case ResultType.SCALA =>
          if (SparderContext.needCompute()) {
            Right(ResultPlan.collectScalarEnumerable(df, rowType))
          } else {
            Right(Linq4j.asEnumerable(Lists.newArrayList[Any]()))
          }
      }
    SparderContext.cleanQueryInfo()
    SparderContext.closeThreadSparkSession()
    result
  }
}

object QueryToExecutionIDCache extends Logging {
  val KYLIN_QUERY_ID_KEY = "kylin.query.id"

  private val queryID2ExecutionID: Cache[String, String] =
    CacheBuilder.newBuilder().maximumSize(1000).build()

  def getQueryExecutionID(queryID: String): String = {
    val executionID = queryID2ExecutionID.getIfPresent(queryID)
    if (executionID == null) {
      logWarning(s"Can not get execution ID by query ID $queryID")
      ""
    } else {
      executionID
    }
  }

  def setQueryExecutionID(queryID: String, executionID: String): Unit = {
    if (queryID != null && !queryID.isEmpty) {
      queryID2ExecutionID.put(queryID, executionID)
    } else {
      logWarning(s"Can not get query ID.")
    }
  }
}