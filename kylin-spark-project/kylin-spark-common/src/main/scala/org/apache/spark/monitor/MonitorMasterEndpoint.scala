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
