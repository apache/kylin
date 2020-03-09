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
