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

package org.apache.spark.utils

import java.util.{Map => JMap}

import org.apache.kylin.cluster.{AvailableResource, IClusterManager, ResourceInfo}
import org.apache.kylin.engine.spark.job.{KylinBuildEnv, SparkJobConstants}
import org.apache.kylin.engine.spark.utils.SparkConfHelper._
import org.apache.spark.SparkConf
import org.apache.spark.application.NoRetryException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.util.{Failure, Success, Try}


object ResourceUtils extends Logging {

  @throws[Exception]
  def caculateRequiredCores(detectItems: JMap[String, String]): String = {
    Try {
      val it = detectItems.entrySet().iterator()
      var pNum = SparkJobConstants.DEFAULT_REQUIRED_CORES
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
        SparkJobConstants.DEFAULT_REQUIRED_CORES
    }
  }

  @throws(classOf[Exception])
  def checkResource(sparkConf: SparkConf, clusterManager: IClusterManager): Boolean = {
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
}
