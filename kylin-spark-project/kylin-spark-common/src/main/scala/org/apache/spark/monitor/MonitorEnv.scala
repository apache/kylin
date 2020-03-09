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
