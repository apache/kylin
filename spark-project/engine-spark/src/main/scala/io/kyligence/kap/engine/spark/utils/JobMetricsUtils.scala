/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.kyligence.kap.engine.spark.utils

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.ui.PostQueryExecutionForKylin


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
    } else {
      logError(s"Collect output rows failed.")
    }
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

  // to get actual QueryExecution when write parquet, more info in issue #8212
  def registerListener(ss: SparkSession): Unit = {
    sparkListener = new SparkListener {

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
