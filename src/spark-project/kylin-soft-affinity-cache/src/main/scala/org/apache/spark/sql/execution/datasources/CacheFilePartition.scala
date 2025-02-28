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

package org.apache.spark.sql.execution.datasources

import org.apache.kylin.softaffinity.SoftAffinityManager
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.InputPartition

/**
 * A collection of file blocks that should be read as a single task
 * (possibly from multiple partitioned directories).
 */
case class CacheFilePartition(index: Int, files: Array[CachePartitionedFile], nativeFilePartition: FilePartition)
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    files.head.locations
  }
}

object CacheFilePartition extends Logging {

  def convertFilePartitionToCache(filePartition: FilePartition): CacheFilePartition = {
    // Get the original preferred locations
    val expectedTargets = filePartition.preferredLocations()
    val files = filePartition.files

    var locations = Array.empty[String]
    if (!files.isEmpty && SoftAffinityManager.usingSoftAffinity
      && !SoftAffinityManager.checkTargetHosts(expectedTargets)) {
      // if there is no host in the node list which are executors running on,
      // using SoftAffinityManager to generate target executors.
      // Only using the first file to calculate the target executors
      locations = SoftAffinityManager.askExecutors(files.head.filePath)
      if (!locations.isEmpty) {
        logInfo(s"SAMetrics=File ${files.head.filePath} - " +
          s"the expected executors are ${locations.mkString("_")} ")
      } else {
        locations = expectedTargets
      }
    } else {
      locations = expectedTargets
    }
    CacheFilePartition(filePartition.index, filePartition.files.map(p => {
      CachePartitionedFile(p.partitionValues, p.filePath, p.start, p.length, locations)
    }), filePartition)
  }
}
