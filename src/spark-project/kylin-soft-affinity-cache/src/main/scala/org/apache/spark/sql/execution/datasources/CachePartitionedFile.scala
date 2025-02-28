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

import org.apache.kylin.cache.fs.CacheFileSystemConstants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkEnv, TaskContext, Partition => RDDPartition}


case class CachePartitionedFile(
                                 partitionValues: InternalRow,
                                 filePath: String,
                                 start: Long,
                                 length: Long,
                                 locations: Array[String] = Array.empty) {
  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues," +
      s" on executor locations ${locations.mkString}"
  }
}

class CacheFileScanRDD(
                        @transient private val sparkSession: SparkSession,
                        readFunction: PartitionedFile => Iterator[InternalRow],
                        @transient val cacheFilePartitions: Seq[CacheFilePartition],
                        override val readDataSchema: StructType,
                        override val metadataColumns: Seq[AttributeReference] = Seq.empty)
  extends FileScanRDD(sparkSession, readFunction, cacheFilePartitions.map { cachePartition =>
    cachePartition.nativeFilePartition
  }, readDataSchema, metadataColumns) {

  def checkCached(cacheLocations: Array[String]): Boolean = {
    val underscoreExecId = "_" + SparkEnv.get.executorId;
    cacheLocations.exists(_.contains(underscoreExecId))
  }

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    // Convert 'CacheFilePartition' to 'FilePartition'
    val start = System.currentTimeMillis()
    val cacheFilePartition = split.asInstanceOf[CacheFilePartition]
    val cacheSplit = FilePartition(cacheFilePartition.index, cacheFilePartition.files.map { f =>
      PartitionedFile(f.partitionValues, f.filePath, f.start, f.length, f.locations)
    })
    var currFilePath = "empty"
    val isCache = if (!cacheFilePartition.files.isEmpty) {
      currFilePath = cacheFilePartition.files.head.filePath
      checkCached(cacheFilePartition.files.head.locations)
    } else {
      logInfo("Cache file partition is empty.")
      false
    }
    logInfo(s"SAMetrics=File $currFilePath running in task ${context.taskAttemptId()} " +
      s"on executor ${SparkEnv.get.executorId} with cached $isCache , " +
      s"took ${System.currentTimeMillis() - start}")
    // Set whether needs to cache data on this executor
    context.getLocalProperties.setProperty(
      CacheFileSystemConstants.PARAMS_KEY_LOCAL_CACHE_FOR_CURRENT_FILES, isCache.toString)
    super.compute(cacheSplit, context)
  }

  override protected def getPartitions: Array[RDDPartition] = cacheFilePartitions.toArray

  override protected def getPreferredLocations(split: RDDPartition): Seq[String] = {
    split.asInstanceOf[CacheFilePartition].preferredLocations()
  }
}
