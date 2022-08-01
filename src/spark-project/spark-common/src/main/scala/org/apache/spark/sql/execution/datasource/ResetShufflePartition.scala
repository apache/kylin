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
package org.apache.spark.sql.execution.datasource

import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

trait ResetShufflePartition extends Logging {

  def setShufflePartitions(bytes: Long, sourceRows: Long, sparkSession: SparkSession): Unit = {
    QueryContext.current().getMetrics.setSourceScanBytes(bytes)
    QueryContext.current().getMetrics.setSourceScanRows(sourceRows)
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val kapConfig = KapConfig.getInstanceFromEnv
    val partitionsNum = if (kapConfig.getSparkSqlShufflePartitions != -1) {
      kapConfig.getSparkSqlShufflePartitions
    } else {
      Math.min(QueryContext.current().getMetrics.getSourceScanBytes / (
        KylinConfig.getInstanceFromEnv.getQueryPartitionSplitSizeMB * 1024 * 1024) + 1,
        defaultParallelism).toInt
    }
    sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", partitionsNum.toString)
    logInfo(s"Set partition to $partitionsNum, total bytes ${QueryContext.current().getMetrics.getSourceScanBytes}")
  }
}
