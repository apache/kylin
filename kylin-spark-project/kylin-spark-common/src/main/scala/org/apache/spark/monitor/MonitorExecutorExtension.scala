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

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.util.RpcUtils
import org.apache.spark.{ExecutorPlugin, SparkConf, SparkEnv}

class MonitorExecutorExtension extends ExecutorPlugin with Logging {

  val env: SparkEnv = SparkEnv.get

  val rpcEnv: RpcEnv = env.rpcEnv

  val sparkConf: SparkConf = env.conf

  override def init(): Unit = {

    initMonitorEnv()

    registerExecutorWithDriver()
  }

  private def initMonitorEnv(): Unit = {
    val driverHost: String = env.conf.get("spark.driver.host", "localhost")
    val driverPort: Int = env.conf.getInt("spark.driver.port", 7077)
    logInfo(s"init monitor env, executorId: ${env.executorId}, driver -> $driverHost : $driverPort")

    MonitorEnv.create(sparkConf, env.executorId, rpcEnv, RpcAddress(driverHost, driverPort), isDriver = false)
    MonitorEnv.get.monitorManager.setMemoryMonitor(MemoryMonitor.install())
  }

  private def registerExecutorWithDriver() = {
    val driverRef = MonitorEnv.get.monitorManager.driverEndpoint
    logInfo(s"register executor executorId : ${env.executorId}")
    val slaverEndpoint = new MonitorSlaverEndpoint(rpcEnv, driverRef)
    val workerRef = rpcEnv.setupEndpoint(MonitorSlaverEndpoint.ENDPOINT_NAME + env.executorId, slaverEndpoint)
    slaverEndpoint.registerMaster(env.executorId, workerRef)
  }

  override def shutdown(): Unit = super.shutdown()

}