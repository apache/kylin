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

import io.kyligence.kap.cluster.{AvailableResource, ClusterInfoFetcher, ResourceInfo}
import io.kyligence.kap.engine.spark.job.SparkJobConstants
import io.kyligence.kap.engine.spark.utils.SparkConfHelper._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.utils.DetectItem
import org.apache.spark.util.Utils

import scala.util.{Failure, Success, Try}


object ResourceUtils extends Logging {

  @throws[Exception]
  def caculateRequiredCores(sampSplitThreshold: String, detectItems: JMap[String, String], rowCount: Long): String = {

    Try {
      val lineCount = detectItems.get(DetectItem.ESTIMATED_LINE_COUNT)
      if (lineCount == "0") {
        logInfo(s"the lineCount is $lineCount")
        return SparkJobConstants.DEFAULT_REQUIRED_CORES
      }
      val estimatedSize = detectItems.get(DetectItem.ESTIMATED_SIZE)
      val splitThreshold = Utils.byteStringAsBytes(sampSplitThreshold)
      val aveBytesSingleLine = estimatedSize.toDouble / lineCount.toDouble
      assert(splitThreshold > aveBytesSingleLine)
      val linesPerPartition = splitThreshold / aveBytesSingleLine
      val partitions = if (linesPerPartition >= rowCount) {
        SparkJobConstants.DEFAULT_REQUIRED_CORES
      } else {
        math.ceil(rowCount / linesPerPartition).toInt
      }
      logInfo(s"linecount is $lineCount, estimatedSize is $estimatedSize, splitThreshold is $splitThreshold")
      logInfo(s"aveBytesSingleLine is $aveBytesSingleLine, linesPerPartition is $linesPerPartition, partitions is $partitions")
      partitions.toString
    } match {
      case Success(partitionNum) =>
        partitionNum
      case Failure(throwable) =>
        logWarning(s"caculate required cores failed ${this.getClass.getName}", throwable)
        return SparkJobConstants.DEFAULT_REQUIRED_CORES
    }
  }


  def checkResource(sparkConf: SparkConf, clusterInfo: ClusterInfoFetcher): Boolean = {
    val queue = sparkConf.get("spark.yarn.queue", "default")
    val driverMemory = (Utils.byteStringAsMb(sparkConf.get(DRIVER_MEMORY)) + Utils.byteStringAsMb(sparkConf.get(DRIVER_OVERHEAD))).toInt
    val driverCores = sparkConf.get(DRIVER_CORES).toInt
    val queueAvailable = minusDriverResource(clusterInfo.fetchQueueAvailableResource(queue), driverMemory, driverCores)
    val instances = sparkConf.get(EXECUTOR_INSTANCES).toInt
    val executorMemory = (Utils.byteStringAsMb(sparkConf.get(EXECUTOR_MEMORY))
      + Utils.byteStringAsMb(sparkConf.get(EXECUTOR_OVERHEAD))) * instances
    val executorCores = sparkConf.get(EXECUTOR_CORES).toInt * instances

    if (!verify(queueAvailable.max, executorMemory, executorCores, instances)) {
      logInfo(s"Require resource ($executorMemory MB, $executorCores vCores)," +
        s" queue max resource (${queueAvailable.max.memory} MB, ${queueAvailable.max.vCores} vCores)")
      throw new RuntimeException("Total queue resource does not meet requirement")
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
}