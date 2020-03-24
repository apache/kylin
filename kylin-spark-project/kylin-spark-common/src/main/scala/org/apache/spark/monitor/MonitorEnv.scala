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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}

import scala.util.control.NonFatal

class MonitorEnv(
  val executorId: String,
  val rpcEnv: RpcEnv,
  val monitorManager: MonitorManager) extends Logging

object MonitorEnv extends Logging {

  private var env: MonitorEnv = _

  def set(e: MonitorEnv) {
    env = e
  }

  /**
   * Returns the MonitorEnv.
   */
  def get: MonitorEnv = {
    env
  }

  /**
   * Helper method to create a Monitor.
   */
  def create(conf: SparkConf, executorId: String, rpcEnv: RpcEnv, rpcAddress: RpcAddress, isDriver: Boolean): MonitorEnv = {
    var endpointRef: RpcEndpointRef = null

    if (isDriver) {
      logInfo("create driver monitor env")
      endpointRef = rpcEnv.setupEndpoint(MonitorMasterEndpoint.ENDPOINT_NAME, new MonitorMasterEndpoint(rpcEnv))
    } else {
      try {
        logInfo(s"create executor $executorId monitor env")
        endpointRef = rpcEnv.setupEndpointRef(rpcAddress, MonitorMasterEndpoint.ENDPOINT_NAME)
      } catch {
        case ie: InterruptedException => // Cancelled
        case NonFatal(e) => logWarning(s"Failed to connect to master", e)
      }
    }

    val monitorManager: MonitorManager = new MonitorManager(endpointRef, conf, false)
    val monitorEnv = new MonitorEnv(executorId, rpcEnv, monitorManager)
    set(monitorEnv)
    monitorEnv
  }
}
