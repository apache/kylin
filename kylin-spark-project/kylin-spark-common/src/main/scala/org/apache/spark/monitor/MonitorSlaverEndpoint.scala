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

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.{Future => JFuture}

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MonitorMessage.{CollectingExecutorMemoryMetrics, CollectingExecutorThreadInfoMetrics, RegisterWorker}
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

import scala.util.control.NonFatal

class MonitorSlaverEndpoint(override val rpcEnv: RpcEnv, driverEndpoint: RpcEndpointRef) extends RpcEndpoint with Logging {

  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-monitor-master-threadpool",
    1
  )

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case CollectingExecutorMemoryMetrics() =>
      context.reply(getExecutorMemoryMetrics())

    case CollectingExecutorThreadInfoMetrics() =>
      context.reply(getExecutorThreadInfoMetrics())
  }

  def getExecutorMemoryMetrics(): MemorySnapshot = {
    val monitorEnv = MonitorEnv.get
    logInfo("start collect executor memory snapshot.")
    monitorEnv.monitorManager.getMemoryMonitor.collectSnapshot
  }

  def getExecutorThreadInfoMetrics(): ThreadInfoSnapshot = {
    val monitorEnv = MonitorEnv.get
    logInfo("start collect executor thread infos.")
    monitorEnv.monitorManager.getMemoryMonitor.collectThreadInfo
  }

  def registerMaster(executorId: String, workerRef: RpcEndpointRef): JFuture[_] = {
    registerMasterThreadPool.submit(
      new Runnable {
        override def run(): Unit = {
          try {
            logInfo(s"registering executor $executorId")
            val updatedId = driverEndpoint.askSync[String](RegisterWorker(executorId, workerRef))
            logInfo(s"registered executor $updatedId")
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"failed to connect to driver ", e)
          }
        }
      }
    )
  }
}

object MonitorSlaverEndpoint {
  val ENDPOINT_NAME = "MonitorSlaver"
}
