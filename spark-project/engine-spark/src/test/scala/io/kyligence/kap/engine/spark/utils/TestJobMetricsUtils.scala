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

package io.kyligence.kap.engine.spark.utils

import java.io.File
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll


class TestJobMetricsUtils extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {

  private val path1 = "./temp1"
  private val id1 = UUID.randomUUID.toString
  private val path2 = "./temp2"
  private val id2 = UUID.randomUUID.toString

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
    val afterJoin = flatTable
    val afterSort = afterJoin.groupBy("key1").agg(count("key3")).repartition(2).sort("key1")
    afterSort.collect()
    val metrics = JobMetricsUtils.collectOutputRows(afterSort.queryExecution.executedPlan)
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
