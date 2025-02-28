/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.utils

import org.apache.kylin.cluster.{AvailableResource, IClusterManager, ResourceInfo}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.{JobConstants, KylinBuildEnv}
import org.apache.kylin.engine.spark.utils.SparkConfHelper._
import org.apache.spark.SparkConf
import org.apache.spark.application.NoRetryException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{DRIVER_MEMORY_OVERHEAD, DRIVER_MEMORY_OVERHEAD_FACTOR, SUBMIT_DEPLOY_MODE, DRIVER_CORES => SPARK_DRIVER_CORES, DRIVER_MEMORY => SPARK_DRIVER_MEMORY, EXECUTOR_CORES => SPARK_EXECUTOR_CORES, EXECUTOR_INSTANCES => SPARK_EXECUTOR_INSTANCES, EXECUTOR_MEMORY => SPARK_EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD => SPARK_EXECUTOR_MEMORY_OVERHEAD, EXECUTOR_MEMORY_OVERHEAD_FACTOR => SPARK_EXECUTOR_MEMORY_OVERHEAD_FACTOR}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.util.Utils

import java.util.{Map => JMap}
import scala.util.{Failure, Success, Try}


object ResourceUtils extends Logging {

  @throws[Exception]
  def caculateRequiredCores(detectItems: JMap[String, String]): String = {
    Try {
      val it = detectItems.entrySet().iterator()
      var pNum = JobConstants.DEFAULT_REQUIRED_CORES
      if (it.hasNext) {
        val item = it.next()
        pNum = item.getValue
        logInfo(s"Require core num is $pNum")
      }
      pNum
    } match {
      case Success(partitionNum) =>
        partitionNum
      case Failure(throwable) =>
        logWarning(s"caculate required cores failed ${this.getClass.getName}", throwable)
        JobConstants.DEFAULT_REQUIRED_CORES
    }
  }

  @throws(classOf[Exception])
  def checkResource(sparkConf: SparkConf, clusterManager: IClusterManager, skipCheckResource: Boolean = false): Boolean = {
    if (skipCheckResource) {
      logInfo("skip check resource.")
      return true
    }
    verifyClusterResource(clusterManager.fetchMaximumResourceAllocation, sparkConf)

    val queue = sparkConf.get("spark.yarn.queue", "default")
    val driverMemory = (Utils.byteStringAsMb(sparkConf.get(DRIVER_MEMORY)) + Utils.byteStringAsMb(sparkConf.get(DRIVER_OVERHEAD))).toInt
    val driverCores = sparkConf.get(DRIVER_CORES).toInt
    val queueAvailable = minusDriverResource(clusterManager.fetchQueueAvailableResource(queue), driverMemory, driverCores)
    val instances = sparkConf.get(EXECUTOR_INSTANCES).toInt
    val executorMemory = (Utils.byteStringAsMb(sparkConf.get(EXECUTOR_MEMORY))
      + Utils.byteStringAsMb(sparkConf.get(EXECUTOR_OVERHEAD))) * instances
    val executorCores = sparkConf.get(EXECUTOR_CORES).toInt * instances

    if (!verify(queueAvailable.max, executorMemory, executorCores, instances)) {
      logInfo(s"Require resource ($executorMemory MB, $executorCores vCores)," +
        s" queue max resource (${queueAvailable.max.memory} MB, ${queueAvailable.max.vCores} vCores)")
      throw new NoRetryException("Total queue resource does not meet requirement")
    }
    logInfo(s"Require resource ($executorMemory MB, $executorCores vCores)," +
      s" available resource (${queueAvailable.available.memory} MB, ${queueAvailable.available.vCores} vCores)")
    verify(queueAvailable.available, executorMemory, executorCores, instances)
  }

  private def verify(resource: ResourceInfo, memory: Long, vCores: Long, instances: Int): Boolean = {
    if (instances == 1) {
      resource.memory >= memory && resource.vCores >= vCores
    } else {
      resource.memory * 1.0 / memory >= 0.5 && resource.vCores * 1.0 / vCores >= 0.5
    }
  }

  private def minusDriverResource(queueAvailable: AvailableResource, memory: Int, vCores: Int): AvailableResource = {
    val am = queueAvailable.available.memory - memory
    val av = queueAvailable.available.vCores - vCores
    val mm = queueAvailable.max.memory - memory
    val mv = queueAvailable.max.vCores - vCores
    AvailableResource(ResourceInfo(am, av), ResourceInfo(mm, mv))
  }

  private def verifyClusterResource(maxResource: ResourceInfo, sparkConf: SparkConf): Unit = {
    val mp = KylinBuildEnv.get().kylinConfig.getMaxAllocationResourceProportion
    val olp = KylinBuildEnv.get().kylinConfig.getSparkEngineResourceRequestOverLimitProportion
    val maxMem = maxResource.memory * mp
    val executorMem = (Utils.byteStringAsMb(sparkConf.get(EXECUTOR_MEMORY)) + Utils.byteStringAsMb(sparkConf.get(EXECUTOR_OVERHEAD))).toInt
    logInfo(s"Verifying our application has not requested s($executorMem MB per executor) more than the maximum allocation " +
      s"memory capability of the cluster ($maxMem MB per container)")
    if (maxMem < executorMem) {
      logInfo(s"Use kylin.engine.resource-request-over-limit-proportion $olp")
      if (maxMem * olp > executorMem) {
        logInfo(s"The maximum memory capability tolerate maximum requested ${maxMem * olp} MB per executor")
        val rp = maxMem * 1.0 / executorMem
        val executorOverhead = (Utils.byteStringAsMb(sparkConf.get(EXECUTOR_OVERHEAD)) * rp).toInt
        val executorMemory = (Utils.byteStringAsMb(sparkConf.get(EXECUTOR_MEMORY)) * rp).toInt
        sparkConf.set(EXECUTOR_OVERHEAD, s"${executorOverhead}MB")
        sparkConf.set(EXECUTOR_MEMORY, s"${executorMemory}MB")
        logInfo(s"Set spark.executor.memoryOverhead to ${executorOverhead}MB")
        logInfo(s"Set spark.executor.memory to ${executorMemory}MB")
      } else {
        throw new NoRetryException(s"Our application has requested s($executorMem MB per executor) more than the maximum allocation " +
          s"memory capability of the cluster $maxMem MB per container")
      }
    }
  }

  def getAllCores(conf: SparkConf, config: KylinConfig): Int = {
    if (isCluster(conf)) {
      getExecutorCores(conf, config) + getDriverCores(conf, config)
    } else {
      getExecutorCores(conf, config)
    }
  }

  def getQueueName(conf: SparkConf): String = {
    var queue = conf.get("spark.kubernetes.scheduler.volcano.podGroup.spec.queue", null)
    if (queue == null) {
      queue = conf.get(DEFAULT_QUEUE, "default")
    }
    queue
  }

  def getAllMemory(conf: SparkConf, config: KylinConfig): Long = {
    if (isCluster(conf)) {
      getExecutorMemory(conf, config) + getDriverMemory(conf, config)
    } else {
      getExecutorMemory(conf, config)
    }
  }

  private def isCluster(conf: SparkConf) = conf.get(SUBMIT_DEPLOY_MODE).equals("cluster")

  private def getDriverCores(conf: SparkConf, config: KylinConfig): Int = {
    math.max(conf.get(SPARK_DRIVER_CORES), config.getContainerMinCore)
  }

  private def getDriverMemory(conf: SparkConf, config: KylinConfig): Long = {
    val driverMemory = conf.get(SPARK_DRIVER_MEMORY)
    val driverMemoryOverhead = conf.get(DRIVER_MEMORY_OVERHEAD).getOrElse(
      math.max((conf.get(DRIVER_MEMORY_OVERHEAD_FACTOR) * driverMemory).toLong,
        ResourceProfile.MEMORY_OVERHEAD_MIN_MIB)).toInt
    math.max(driverMemory + driverMemoryOverhead, config.getContainerMinMB)
  }

  private def getExecutorCores(conf: SparkConf, config: KylinConfig): Int = {
    val executorCores = conf.get(SPARK_EXECUTOR_CORES)
    math.max(executorCores, config.getContainerMinCore) * getExecutors(conf)
  }

  private def getExecutorMemory(conf: SparkConf, config: KylinConfig): Long = {
    val executorMemory = conf.get(SPARK_EXECUTOR_MEMORY)
    val executorOffHeapMemory = Utils.executorOffHeapMemorySizeAsMb(conf)
    val executorMemoryOverhead = conf.get(SPARK_EXECUTOR_MEMORY_OVERHEAD).getOrElse(
      math.max((conf.get(SPARK_EXECUTOR_MEMORY_OVERHEAD_FACTOR) * executorMemory).toLong,
        ResourceProfile.MEMORY_OVERHEAD_MIN_MIB)).toInt
    math.max(executorMemory + executorMemoryOverhead + executorOffHeapMemory, config.getContainerMinMB) * getExecutors(conf)
  }

  private def getExecutors(conf: SparkConf): Int = conf.get(SPARK_EXECUTOR_INSTANCES).getOrElse(0)
}
