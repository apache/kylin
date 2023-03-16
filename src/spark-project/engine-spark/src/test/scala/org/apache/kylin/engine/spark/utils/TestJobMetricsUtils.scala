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

package org.apache.kylin.engine.spark.utils

import org.apache.commons.io.FileUtils
import org.apache.kylin.common.util.RandomUtil
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}


class TestJobMetricsUtils extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll
  with LocalMetadata {

  private val path1 = "./temp1"
  private val id1 = RandomUtil.randomUUIDStr
  private val path2 = "./temp2"
  private val id2 = RandomUtil.randomUUIDStr

  private var flatTable: DataFrame = _

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(new File(path1))
    FileUtils.deleteQuietly(new File(path2))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val df1 = generateTable1()
    val df2 = generateTable2()
    flatTable = df1.join(df2, col("key2") === col("key4"))
  }

  test("collectOutputRows - mock build agg index with persist") {
    JobMetricsUtils.registerListener(spark)
    spark.sparkContext.setLocalProperty("spark.sql.execution.id", id1)
    val afterSort = flatTable.groupBy("key1").agg(count("key3").alias("key3_count")).repartition(2).sort("key1")
    val metrics = StorageUtils.writeWithMetrics(afterSort, "/tmp/" + RandomUtil.randomUUID().toString)
    assert(metrics.getMetrics(Metrics.CUBOID_ROWS_CNT) == 10)
    assert(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT) == 50)
  }

  test("collectOutputRows - mock build agg index without persist") {
    val afterSort = flatTable.groupBy("key1").agg(count("key3")).repartition(2).sort("key1")
    afterSort.collect()
    val metrics = JobMetricsUtils.collectOutputRows(afterSort.queryExecution.executedPlan)
    assert(metrics.getMetrics(Metrics.CUBOID_ROWS_CNT) == 10)
    assert(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT) == 50)
  }

  test("collectOutputRows - mock build table index") {
    val afterSort = flatTable.repartition(2).sort("key1")
    afterSort.collect()
    val metrics = JobMetricsUtils.collectOutputRows(afterSort.queryExecution.executedPlan)
    assert(metrics.getMetrics(Metrics.CUBOID_ROWS_CNT) == 50)
    assert(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT) == 50)
  }

  test("collectOutputRows - mock merge table index") {
    val df1 = generateTable1()
    val df2 = generateTable2()
    val afterUnion = df1.union(df2)
    afterUnion.collect()
    val metrics = JobMetricsUtils.collectOutputRows(afterUnion.queryExecution.executedPlan)
    assert(metrics.getMetrics(Metrics.CUBOID_ROWS_CNT) == 20)
    assert(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT) == 20)
  }

  test("collectOutputRows - mock merge table index with aggregate") {
    JobMetricsUtils.registerListener(spark)
    spark.sparkContext.setLocalProperty("spark.sql.execution.id", id1)
    val df1 = generateTable1()
    val df2 = generateTable2().agg(Map( "key3" -> "max", "key4" -> "min"))
    val afterUnion = df1.union(df2)
    val metrics = StorageUtils.writeWithMetrics(afterUnion, "/tmp/" + RandomUtil.randomUUID().toString)
    assert(metrics.getMetrics(Metrics.CUBOID_ROWS_CNT) == 11)
    assert(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT) == 20)
  }

  test("collectMetrics - mock parallel write parquet") {
    JobMetricsUtils.registerListener(spark)
    val service = Executors.newFixedThreadPool(2)
    service.execute(new NRunnable(path1, id1))
    service.execute(new NRunnable(path2, id2))
    try {
      service.shutdown()
      service.awaitTermination(10, TimeUnit.SECONDS)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
        service.shutdownNow()
    }
    val metrics1 = JobMetricsUtils.collectMetrics(id1)
    assert(metrics1.getMetrics(Metrics.CUBOID_ROWS_CNT) == 10)
    assert(metrics1.getMetrics(Metrics.SOURCE_ROWS_CNT) == 50)

    val metrics2 = JobMetricsUtils.collectMetrics(id2)
    assert(metrics2.getMetrics(Metrics.CUBOID_ROWS_CNT) == 10)
    assert(metrics2.getMetrics(Metrics.SOURCE_ROWS_CNT) == 50)
  }

  class NRunnable(path: String, executionId: String) extends Runnable {
    override def run(): Unit = {
      val afterJoin = flatTable
      val afterSort = afterJoin.groupBy("key1").agg(count("key3").as("count_key3")).repartition(2).sort("key1")

      spark.sparkContext.setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY, executionId)
      afterSort.write
        .mode(SaveMode.Overwrite)
        .parquet(path)
      logInfo(s"Write data to $path done.")
      spark.sparkContext.setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY, null)
    }
  }

  def generateTable1(): Dataset[Row] = {
    var schema = new StructType
    schema = schema.add("key1", StringType)
    schema = schema.add("key2", StringType)
    val data = Seq(
      Row("0", "a"),
      Row("1", "b"),
      Row("2", "a"),
      Row("3", "b"),
      Row("4", "a"),
      Row("5", "b"),
      Row("6", "a"),
      Row("7", "b"),
      Row("8", "a"),
      Row("9", "b"))

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  def generateTable2(): Dataset[Row] = {
    var schema = new StructType
    schema = schema.add("key3", StringType)
    schema = schema.add("key4", StringType)
    val data = Seq(
      Row("10", "a"),
      Row("11", "b"),
      Row("12", "a"),
      Row("13", "b"),
      Row("14", "a"),
      Row("15", "b"),
      Row("16", "a"),
      Row("17", "b"),
      Row("18", "a"),
      Row("19", "b"))

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }
}
