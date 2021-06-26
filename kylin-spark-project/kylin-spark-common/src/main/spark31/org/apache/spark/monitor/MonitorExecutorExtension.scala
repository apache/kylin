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

import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.{SparkConf, SparkEnv}

class MonitorExecutorExtension extends ExecutorPlugin with Logging {

  val env: SparkEnv = SparkEnv.get

  val rpcEnv: RpcEnv = env.rpcEnv

  val sparkConf: SparkConf = env.conf

  override def init(pluginContext: PluginContext, extraConf: util.Map[String, String]): Unit = {

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