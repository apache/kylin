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
package org.apache.spark.memory

import java.util

import org.apache.kylin.shaded.com.google.common.collect.Maps
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

object MetricsCollectHelper extends Logging {

  def getMemorySnapshot(): util.Map[String, util.List[String]] = {
    logInfo("Start getMetricsMemory...")
    val memorySnapshotMap = Maps.newHashMap[String, util.List[String]]()
    val monitorEnv = MonitorEnv.get
    val memorySnapshot = monitorEnv.monitorManager.getMemoryMetrics
    memorySnapshot.entrySet().asScala foreach (
      item => {
        logInfo("Memory report executorId:" + item.getKey)
        memorySnapshotMap.put(item.getKey, monitorEnv.monitorManager.getMemoryMonitor.formatSnapshot(item.getValue))
      })
    memorySnapshotMap
  }

  def getThreadInfoSnapshot(): util.Map[String, util.List[String]] = {
    logInfo("Start getMetricsThreadInfo...")
    val threadInfoFormatMap = Maps.newHashMap[String, util.List[String]]()
    val monitorEnv = MonitorEnv.get
    val threadInfoMap = monitorEnv.monitorManager.getThreadInfoMetrics
    threadInfoMap.entrySet().asScala foreach (
      item => {
        logInfo("Thread info report executorId:" + item.getKey)
        threadInfoFormatMap.put(item.getKey, monitorEnv.monitorManager.getMemoryMonitor.formatThreadDump(item.getValue))
      })
    threadInfoFormatMap
  }
}
