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

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MonitorMessage._
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

import scala.collection.mutable

class MonitorMasterEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint with Logging {

  var executors: mutable.HashMap[String, RpcEndpointRef] = new mutable.HashMap[String, RpcEndpointRef]()

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case CollectingMemoryReport() =>
      logInfo("start collect metrics memory.")
      context.reply(collectMemoryReport())

    case CollectingThreadInfoReport() =>
      logInfo("start collect metrics thread info.")
      context.reply(collectThreadInfoReport())

    case RegisterWorker(executorId, workerRef) =>
      logInfo("executor id: " + executorId)
      if (executors.contains(executorId)) {
        logInfo(s"executor id: $executorId already exists.")
      } else {
        logInfo(s"executor id: $executorId registering.")
        executors.put(executorId, workerRef)
        context.reply(executorId)
        logInfo(s"executor id: $executorId register success.")
      }
  }

  private def collectMemoryReport() = {
    val memSnapMap = new util.HashMap[String, MemorySnapshot]()
    executors.foreach {
      case (executorId, workerRef) =>
        logInfo(s"start active collect executor:$executorId metrics memory.")
        val memorySnapshot = workerRef.askSync[MemorySnapshot](CollectingExecutorMemoryMetrics())
        memSnapMap.put(executorId, memorySnapshot)
    }
    memSnapMap
  }

  private def collectThreadInfoReport() = {
    val threadInfos = new util.HashMap[String, ThreadInfoSnapshot]()
    executors.foreach {
      case (executorId, workerRef) =>
        logInfo(s"start active collect executor:$executorId metrics thread info.")
        val info = workerRef.askSync[ThreadInfoSnapshot](CollectingExecutorThreadInfoMetrics())
        threadInfos.put(executorId, info)
    }
    threadInfos
  }
}

object MonitorMasterEndpoint {
  val ENDPOINT_NAME = "MonitorMaster"
}
