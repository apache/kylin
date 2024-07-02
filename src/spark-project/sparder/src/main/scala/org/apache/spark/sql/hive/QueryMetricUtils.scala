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

import org.apache.gluten.execution.{BatchScanExecTransformer, FileSourceScanExecTransformer}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.AppStatus
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.hive.execution.HiveTableScanExec

import scala.collection.JavaConverters._

object QueryMetricUtils extends Logging {
  def collectScanMetrics(plan: SparkPlan): (java.util.List[java.lang.Long], java.util.List[java.lang.Long]) = {
    try {
      val metrics = collectAdaptiveSparkPlanExecMetrics(plan, 0L, 0L)
      val scanRows = Array(new java.lang.Long(metrics._1)).toList.asJava
      val scanBytes = Array(new java.lang.Long(metrics._2)).toList.asJava
      (scanRows, scanBytes)
    } catch {
      case throwable: Throwable =>
        logWarning("Error occurred when collect query scan metrics.", throwable)
        (null, null)
    }
  }

  def collectAdaptiveSparkPlanExecMetrics(exec: SparkPlan, scanRow: scala.Long,
                                          scanBytes: scala.Long): (scala.Long, scala.Long) = {
    exec match {
      case exec: LayoutFileSourceScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case exec: KylinFileSourceScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case transformer: FileSourceScanExecTransformer => // for native file scan
        (scanRow + transformer.metrics.apply("numOutputRows").value, scanBytes + transformer.metrics.apply("outputBytes").value)
      case exec: FileSourceScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case exec: KylinStorageScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case exec: HiveTableScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case transformer: HiveTableScanExecTransformer =>
        (scanRow + transformer.metrics.apply("numOutputRows").value, scanBytes + transformer.metrics.apply("outputBytes").value)
      case exec: BatchScanExec =>
        // avoid empty metrics
        val readBytes = exec.metrics.get("readBytes").map(_.value).getOrElse(0L)
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + readBytes)
      case transformer: BatchScanExecTransformer =>
        // avoid empty metrics
        val numOutputRows = transformer.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
        val outputBytes = transformer.metrics.get("outputBytes").map(_.value).getOrElse(0L)
        (scanRow + numOutputRows, scanBytes + outputBytes)
      case exec: ShuffleQueryStageExec =>
        collectAdaptiveSparkPlanExecMetrics(exec.plan, scanRow, scanBytes)
      case exec: AdaptiveSparkPlanExec =>
        collectAdaptiveSparkPlanExecMetrics(exec.executedPlan, scanRow, scanBytes)
      case exec: Any =>
        var newScanRow = scanRow
        var newScanBytes = scanBytes
        exec.children.foreach(
          child => {
            if (child.isInstanceOf[SparkPlan]) {
              val result = collectAdaptiveSparkPlanExecMetrics(child, scanRow, scanBytes)
              newScanRow += result._1
              newScanBytes += result._2
            } else {
              logTrace("Not sparkPlan in collectAdaptiveSparkPlanExecMetrics, child: " + child.getClass.getName)
            }
          }
        )
        (newScanRow, newScanBytes)
    }
  }

  def collectTaskRelatedMetrics(jobGroup: String, sparkContext: SparkContext): (java.lang.Long, java.lang.Long, java.lang.Long) = {
    try {
      val appStatus = new AppStatus(sparkContext)
      val jobData = appStatus.getJobData(jobGroup)
      val jobCount = jobData.size
      val stageCount = jobData.flatMap(_.stageIds).size
      val taskCount = jobData.map(_.numTasks).sum
      (jobCount, stageCount, taskCount)
    } catch {
      case throwable: Throwable =>
        logWarning("Error occurred when collect query task related metrics.", throwable)
        (0, 0, 0)
    }
  }
}
