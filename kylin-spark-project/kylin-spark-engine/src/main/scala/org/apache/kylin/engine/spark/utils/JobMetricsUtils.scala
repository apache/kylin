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

package org.apache.kylin.engine.spark.utils

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinExec

object JobMetricsUtils extends Logging {

  private val aggs = List(classOf[HashAggregateExec], classOf[SortAggregateExec], classOf[ObjectHashAggregateExec])
  private val joins = List(classOf[BroadcastHashJoinExec], classOf[ShuffledHashJoinExec], classOf[SortMergeJoinExec],
    classOf[BroadcastNestedLoopJoinExec], classOf[StreamingSymmetricHashJoinExec])
  var sparkListener : SparkListener = _
  def collectMetrics(executionId: String): JobMetrics = {
    var metrics = new JobMetrics
    val execution = QueryExecutionCache.getQueryExecution(executionId)
    if (execution != null) {
      metrics = collectOutputRows(execution.executedPlan)
      logInfo(s"Collect output rows successfully. $metrics")
    }

    // comment below source, because it always collect failed when using apache spark.

    // else {
    // logDebug(s"Collect output rows failed.")
    //}
    metrics
  }

  def collectOutputRows(sparkPlan: SparkPlan): JobMetrics = {
    val rowMetrics = new JobMetrics
    var afterAgg = false
    var afterJoin = false

    sparkPlan foreach {
      case plan: UnaryExecNode =>
        if (aggs.contains(plan.getClass) && !afterAgg) {
          afterAgg = true
          rowMetrics.setMetrics(Metrics.CUBOID_ROWS_CNT, plan.metrics.apply("numOutputRows").value)
        }
      case plan: BinaryExecNode =>
        if (joins.contains(plan.getClass) && !afterJoin) {
          rowMetrics.setMetrics(Metrics.SOURCE_ROWS_CNT, plan.metrics.apply("numOutputRows").value)
          afterJoin = true
        }
      case plan: LeafExecNode =>
        if (!afterJoin) {
          // add all numOutputRows in leaf nodes up for union case when merge table index
          val preCnt = if (rowMetrics.getMetrics(Metrics.SOURCE_ROWS_CNT) == -1) {
            0
          } else {
            rowMetrics.getMetrics(Metrics.SOURCE_ROWS_CNT)
          }

          val rowsCnt = preCnt + plan.metrics.apply("numOutputRows").value
          rowMetrics.setMetrics(Metrics.SOURCE_ROWS_CNT, rowsCnt)
        }
      case _ =>
    }

    // resolve table index without agg
    if (!rowMetrics.isDefinedAt(Metrics.CUBOID_ROWS_CNT)) {
      require(!afterAgg)
      rowMetrics.setMetrics(Metrics.CUBOID_ROWS_CNT, rowMetrics.getMetrics(Metrics.SOURCE_ROWS_CNT))
    }

    rowMetrics
  }

  /**
   * When using a custom spark which sent event which contain QueryExecution belongs to a specific N_EXECUTION_ID_KEY,
   * kylin can cache QueryExecution object into QueryExecutionCache and collect metrics such as bytes/row count for a cuboid
   *
     override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case e: PostQueryExecutionForKylin =>
          val nExecutionId = e.localProperties.getProperty(QueryExecutionCache.N_EXECUTION_ID_KEY, "")
          if (nExecutionId != "" && e.queryExecution != null) {
            QueryExecutionCache.setQueryExecution(nExecutionId, e.queryExecution)
          } else {
            logWarning("executionIdStr is null, can't get QueryExecution from SQLExecution.")
          }
        case _ => // Ignore
      }
   */
  def registerListener(ss: SparkSession): Unit = {
    sparkListener = new SparkListener {

      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case _ => // Ignore
      }
    }
    ss.sparkContext.addSparkListener(sparkListener)
  }

  def unRegisterListener(ss: SparkSession) : Unit = {
    if (sparkListener != null) {
      ss.sparkContext.removeSparkListener(sparkListener)
    }
  }
}

object QueryExecutionCache extends Logging {
  val N_EXECUTION_ID_KEY = "kylin.query.execution.id"

  private val executionIdToQueryExecution = new ConcurrentHashMap[String, QueryExecution]()

  def getQueryExecution(executionId: String): QueryExecution = {
    if (executionId != null) {
      executionIdToQueryExecution.get(executionId)
    } else {
      null
    }
  }

  def setQueryExecution(executionId: String, queryExecution: QueryExecution): Unit = {
    if (executionId != null) {
      executionIdToQueryExecution.put(executionId, queryExecution)
    } else {
      logWarning("kylin.query.execution.id is null, don't put QueryExecution into QueryExecutionCache.")
    }
  }

  def removeQueryExecution(executionId: String): Unit = {
    executionIdToQueryExecution.remove(executionId)
  }

}
