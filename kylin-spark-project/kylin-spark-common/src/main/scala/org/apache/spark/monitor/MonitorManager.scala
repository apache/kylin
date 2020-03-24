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