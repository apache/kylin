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

package org.apache.spark.utils

import java.util.{Map => JMap}

import org.apache.kylin.engine.spark.utils.SparkConfHelper._
import org.apache.kylin.cluster.{AvailableResource, ClusterInfoFetcher, ResourceInfo}
import org.apache.kylin.engine.spark.job.SparkJobConstants
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
    var driverMemory = Utils.byteStringAsMb(sparkConf.get(DRIVER_MEMORY)).toInt
    if (sparkConf.contains(DRIVER_OVERHEAD)) {
      driverMemory = driverMemory + Utils.byteStringAsMb(sparkConf.get(DRIVER_OVERHEAD)).toInt
    }
    val driverCores = sparkConf.get(DRIVER_CORES, "1").toInt
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