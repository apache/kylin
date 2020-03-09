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

package org.apache.spark.memory

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MonitorMessage.{CollectingMemoryReport, CollectingThreadInfoReport}
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.util.RpcUtils

class MonitorManager(var driverEndpoint: RpcEndpointRef, conf: SparkConf, isDriver: Boolean) extends Logging {

  private var _monitor: MemoryMonitor = new MemoryMonitor()

  def getMemoryMetrics(): util.Map[String, MemorySnapshot] = {
    driverEndpoint.askSync[util.Map[String, MemorySnapshot]](CollectingMemoryReport())
  }

  def getThreadInfoMetrics(): util.Map[String, ThreadInfoSnapshot] = {
    driverEndpoint.askSync[util.Map[String, ThreadInfoSnapshot]](CollectingThreadInfoReport())
  }

  private[spark] def setMemoryMonitor(monitorMonitor: MemoryMonitor): Unit = {
    _monitor = monitorMonitor
  }

  private[spark] def getMemoryMonitor: MemoryMonitor = {
    _monitor
  }
}