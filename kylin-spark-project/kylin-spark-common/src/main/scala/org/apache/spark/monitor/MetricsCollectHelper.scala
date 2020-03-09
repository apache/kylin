/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */
package org.apache.spark.memory

import java.util

import com.google.common.collect.Maps
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
