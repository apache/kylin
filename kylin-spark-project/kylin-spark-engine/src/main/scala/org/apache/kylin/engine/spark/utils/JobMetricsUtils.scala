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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinExec

object JobMetricsUtils extends Logging {

  private val aggs = List(classOf[HashAggregateExec], classOf[SortAggregateExec], classOf[ObjectHashAggregateExec])
  private val joins = List(classOf[BroadcastHashJoinExec], classOf[ShuffledHashJoinExec], classOf[SortMergeJoinExec],
    classOf[BroadcastNestedLoopJoinExec], classOf[StreamingSymmetricHashJoinExec])

  private val executionIdToListener = new ConcurrentHashMap[String, QueryExecutionInterceptListener]()

  def collectMetrics(executionId: String): JobMetrics = {
    var metrics = new JobMetrics
    val listener = executionIdToListener.getOrDefault(executionId,null)
    if (listener != null && listener.queryExecution.isDefined) {
      metrics = collectOutputRows(listener.queryExecution.get.executedPlan)
      logInfo(s"Collect output rows successfully. $metrics")
    } else {
      logInfo(s"Collect output rows failed.")
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

  def registerQueryExecutionListener(ss: SparkSession, executionId: String): Unit = {
    val listener = new QueryExecutionInterceptListener(executionId)
    executionIdToListener.put(executionId, listener)
    ss.listenerManager.register(listener)
  }

  def unRegisterQueryExecutionListener(ss: SparkSession, executionId: String) : Unit = {
    val listener =  executionIdToListener.remove(executionId)
    if (listener != null) {
      ss.listenerManager.unregister(listener)
    }
  }
}

