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
package org.apache.spark.sql.execution.gluten

import org.apache.gluten.execution.FileSourceScanExecTransformer
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasource.{KylinSourceStrategy, LayoutFileSourceStrategy}

class GlutenFileScanSuite extends SparkFunSuite
  with SQLHelper with AdaptiveSparkPlanHelper {

  override def beforeEach(): Unit = {
    clearSparkSession()
  }

  override def afterEach(): Unit = {
    clearSparkSession()
  }

  // use kylin-it and kap-it to test with gluten
  ignore("Test with Gluten") {
    withTempPath { path =>
      SparkSession.cleanupAnyExistingSession()
      val tempDir = path.getCanonicalPath
      val spark = SparkSession.builder()
        .master("local[3]")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.io.compression.codec", "LZ4")
        .config("spark.gluten.sql.columnar.libpath", "/usr/local/clickhouse/lib/libch.so")
        .config("spark.gluten.sql.enable.native.validation", "false")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2G")
        .config("spark.gluten.sql.columnar.extended.columnar.pre.rules",
          "org.apache.spark.sql.execution.gluten.ConvertKylinFileSourceToGlutenRule")
        .config("spark.gluten.sql.columnar.extended.columnar.post.rules", "")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        }
        .getOrCreate()

      withSQLConf(("spark.gluten.enabled", "false")) {
        createSimpleDF(spark, tempDir)
      }
      val df = spark.read.parquet(tempDir)
        .where("a = 0")
        .groupBy("b")
        .agg("c" -> "sum")

      val res = df.collect()
      assert(res.size == 3)
      val f = getNativeFileSourceScanExecTransformer(df)
      assert(!f.isEmpty)
      assert(f.get.isInstanceOf[FileSourceScanExecTransformer])
      spark.sparkContext.stop()
    }
  }

  private def createSimpleDF(spark: SparkSession, tempDir: String) = {
    spark.range(100)
      .selectExpr("id % 2 as a", "id % 3 as b", "id as c")
      .write
      .parquet(tempDir)
  }

  private def getNativeFileSourceScanExecTransformer(df: DataFrame) = {
    collectFirst(df.queryExecution.executedPlan) {
      case p: KylinFileSourceScanExecTransformer => p
      case p: FileSourceScanExecTransformer => p
    }
  }

  protected def clearSparkSession(): Unit = {
    SparkSession.setActiveSession(null)
    SparkSession.setDefaultSession(null)
    SparkSession.cleanupAnyExistingSession()
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    assertEmptyMissingInput(analyzedDF)

    QueryTest.checkAnswer(analyzedDF, expectedAnswer, true)
  }

  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }

}
