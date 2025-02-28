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
package org.apache.spark.sql.datasource.storage

import org.apache.kylin.common.{KylinConfig, QueryContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.internal.SQLConf

object RewriteRuntimeConfig extends Logging {

  def adjustQueryRuntimeConfigs(spark: SparkSession, files: Seq[AddFile]) {
    val queryStats = getQueryStats(files)
    recordQueryMetrics(queryStats)

    rewriteShufflePartitionSize(spark, queryStats)
  }

  def calculatePartitionNumber(queryStats: QueryStats,
                               fileNumSplit: Long,
                               recordNumSplit: Long,
                               partitionSizeSplit: Long,
                               minPartitionSize: Long): Int = {
    val partitionNumByFileNum = queryStats.fileNum / fileNumSplit + 1
    val partitionNumByRecordNum = queryStats.totalRecords / recordNumSplit + 1
    val partitionNumBySize = queryStats.totalSize / (partitionSizeSplit * 1024 * 1024) + 1
    List(partitionNumByFileNum, partitionNumByRecordNum, partitionNumBySize, minPartitionSize).max.toInt
  }

  def rewriteShufflePartitionSize(spark: SparkSession, queryStats: QueryStats): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val minPartitionSize = config.getV3ScanMinPartitionNum
    val fileNumSplit = config.getV3ScanSplitFileNum
    val recordNumSplit = config.getV3ScanSplitRecordNum
    val partitionSizeSplit = config.getV3ScanSplitSizeMB

    val computedPartitionNum = calculatePartitionNumber(queryStats, fileNumSplit, recordNumSplit, partitionSizeSplit, minPartitionSize)

    val lastConfigSize = QueryContext.current().getShufflePartitionsReset
    val finalPartitionSize = List(computedPartitionNum, lastConfigSize).max

    spark.sessionState.conf.setLocalProperty(SQLConf.SHUFFLE_PARTITIONS.key, finalPartitionSize.toString)
    QueryContext.current().setShufflePartitionsReset(finalPartitionSize)

    logInfo(
      s"""
         |Adjust spark.sql.shuffle.partitions = ${finalPartitionSize}
         |Partition by file num: ${queryStats.fileNum / fileNumSplit + 1}
         |Partition by file size: ${queryStats.totalSize / (partitionSizeSplit * 1024 * 1024) + 1}
         |Partition by record num: ${queryStats.totalRecords / recordNumSplit + 1}
         |Min partition size: ${minPartitionSize}
    """.stripMargin)
  }

  private def recordQueryMetrics(stats: QueryStats): Unit = {
    // Query metrics
    QueryContext.current().getMetrics.setSourceScanBytes(stats.totalSize)
    QueryContext.current().getMetrics.setSourceScanRows(stats.totalRecords)
  }

  private def getQueryStats(files: Seq[AddFile]): QueryStats = {
    val totalRecords = getTotalNumRecords(files)
    val totalSize = getTotalFileSize(files)
    val fileNum = files.size
    QueryStats(fileNum, totalRecords, totalSize)
  }

  private def getTotalNumRecords(files: Seq[AddFile]): Long = {
    files.map(_.numPhysicalRecords.get).sum
  }

  private def getTotalFileSize(files: Seq[AddFile]): Long = {
    files.map(_.size).sum
  }
}

case class QueryStats(fileNum: Long, totalRecords: Long, totalSize: Long)
