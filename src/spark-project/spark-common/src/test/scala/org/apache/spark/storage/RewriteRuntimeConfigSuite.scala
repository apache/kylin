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
package org.apache.spark.storage

import org.apache.kylin.common.{KylinConfig, QueryContext}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.datasource.storage.{QueryStats, RewriteRuntimeConfig}
import org.apache.spark.sql.internal.SQLConf

class RewriteRuntimeConfigSuite extends SparderBaseFunSuite
  with SharedSparkSession with LocalMetadata {

  def getTestConfig: KylinConfig = {
    KylinConfig.getInstanceFromEnv
  }

  test("calculatePartitionNumber should calculate correct partition number") {
    val queryStats = QueryStats(fileNum = 100, totalRecords = 10000, totalSize = 1024 * 1024)
    val fileNumSplit = 10
    val recordNumSplit = 1000
    val partitionSizeSplit = 64

    val result = RewriteRuntimeConfig.calculatePartitionNumber(queryStats,
      fileNumSplit, recordNumSplit, partitionSizeSplit, minPartitionSize = 1)

    assert(result == 11)
  }

  test("calculatePartitionNumber should handle zero inputs") {
    val queryStats = QueryStats(fileNum = 0, totalRecords = 0, totalSize = 0)
    val fileNumSplit = 10
    val recordNumSplit = 1000
    val partitionSizeSplit = 64

    val result = RewriteRuntimeConfig.calculatePartitionNumber(queryStats,
      fileNumSplit, recordNumSplit, partitionSizeSplit, minPartitionSize = 1)

    assert(result == 1)
  }

  test("rewrite shuffle partition size") {
    val session = spark.newSession()
    val tuples = List(
      (QueryStats(1, 1, 1), 1),
      (QueryStats(10, 10000, 100000), 3),
      (QueryStats(1, 10000000, 1000000), 6),
      (QueryStats(10, 10000, 1000000000), 15)
    )

    tuples.foreach { case (query, expectedPartitions) =>
      QueryContext.current().setShufflePartitionsReset(0)
      overwriteSystemProp("kylin.query.v3.scan-min-partition-num", "1")
      RewriteRuntimeConfig.rewriteShufflePartitionSize(session, query)
      assert(session.sessionState.conf.getConf(SQLConf.SHUFFLE_PARTITIONS) === expectedPartitions)
    }
  }

  test("rewrite shuffle partition size with min partition size") {
    QueryContext.current().setShufflePartitionsReset(0)
    val session = spark.newSession()
    val queries = List(
      ("5", QueryStats(1, 1, 1), 5),
      ("100", QueryStats(1, 1, 1), 100)
    )

    queries.foreach { case (minPartitionNum, query, expectedPartitions) =>
      overwriteSystemProp("kylin.query.v3.scan-min-partition-num", minPartitionNum)
      RewriteRuntimeConfig.rewriteShufflePartitionSize(session, query)
      assert(session.sessionState.conf.getConf(SQLConf.SHUFFLE_PARTITIONS) === expectedPartitions)
    }
  }

  test("rewrite shuffle partition size with multi QueryContext") {
    QueryContext.current().setShufflePartitionsReset(0)
    val session = spark.newSession()
    val queries = List(
      ("5", QueryStats(1, 1, 1), 5),
      ("100", QueryStats(1, 1, 1), 100)
    )

    queries.foreach { case (minPartitionNum, query, expectedPartitions) =>
      QueryContext.current().setShufflePartitionsReset(minPartitionNum.toInt)
      RewriteRuntimeConfig.rewriteShufflePartitionSize(session, query)
      assert(session.sessionState.conf.getConf(SQLConf.SHUFFLE_PARTITIONS) === expectedPartitions)
    }
  }

}
