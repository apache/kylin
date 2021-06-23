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

package org.apache.spark.sql.hive.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.{FileSourceScanExec, KylinFileSourceScanExec, SparkPlan}
import org.apache.spark.sql.hive.execution.HiveTableScanExec

import scala.collection.JavaConverters._

object QueryMetricUtils extends Logging {
  def collectScanMetrics(plan: SparkPlan): (java.util.List[java.lang.Long], java.util.List[java.lang.Long],
          java.util.List[java.lang.Long], java.util.List[java.lang.Long], java.util.List[java.lang.Long]) = {
    try {
      val metrics = plan.collect {
        case exec: KylinFileSourceScanExec =>
          //(exec.metrics.apply("numOutputRows").value, exec.metrics.apply("readBytes").value)
          (exec.metrics.apply("numOutputRows").value, exec.metrics.apply("numFiles").value,
                  exec.metrics.apply("metadataTime").value, exec.metrics.apply("scanTime").value, -1l)
        case exec: FileSourceScanExec =>
          //(exec.metrics.apply("numOutputRows").value, exec.metrics.apply("readBytes").value)
          (exec.metrics.apply("numOutputRows").value, exec.metrics.apply("numFiles").value,
                  exec.metrics.apply("metadataTime").value, exec.metrics.apply("scanTime").value, -1l)
        case exec: HiveTableScanExec =>
          //(exec.metrics.apply("numOutputRows").value, exec.metrics.apply("readBytes").value)
          // There is only 'numOutputRows' metric in HiveTableScanExec
          (exec.metrics.apply("numOutputRows").value, -1l, -1l, -1l, -1l)
      }

      val scanRows = metrics.map(metric => java.lang.Long.valueOf(metric._1))
        .filter(_ >= 0L).toList.asJava
      val scanFiles = metrics.map(metrics => java.lang.Long.valueOf(metrics._2))
        .filter(_ >= 0L).toList.asJava
      val metadataTime = metrics.map(metrics => java.lang.Long.valueOf(metrics._3))
        .filter(_ >= 0L).toList.asJava
      val scanTime = metrics.map(metrics => java.lang.Long.valueOf(metrics._4))
        .filter(_ >= 0L).toList.asJava
      val scanBytes = metrics.map(metric => java.lang.Long.valueOf(metric._5))
        .filter(_ >= 0L).toList.asJava

      (scanRows, scanFiles, metadataTime, scanTime, scanBytes)
    } catch {
      case throwable: Throwable =>
        logWarning("Error occurred when collect query scan metrics.", throwable)
        (List.empty[java.lang.Long].asJava, List.empty[java.lang.Long].asJava, List.empty[java.lang.Long].asJava,
                List.empty[java.lang.Long].asJava, List.empty[java.lang.Long].asJava)
    }
  }
}
