/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.StringUtils
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.hadoop.fs.Path
import org.apache.kylin.cache.softaffinity.SoftAffinityConstants
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.common.LocalMetadata
import org.apache.spark.sql.delta.KylinDeltaLogFileIndex
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasource._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.gluten.KylinStorageScanExecTransformer
import org.mockito.{ArgumentMatchers, Mockito}

import com.google.common.cache.CacheBuilder

class KylinFileSourceScanExecSuite extends SparkFunSuite
  with SQLHelper with AdaptiveSparkPlanHelper with LocalMetadata {

  override def beforeEach(): Unit = {
    clearSparkSession()
  }

  override def afterEach(): Unit = {
    clearSparkSession()
  }

  test("Create sharding read RDD with Soft affinity - CacheFileScanRDD") {
    SparkSession.cleanupAnyExistingSession()
    val spark = SparkSession.builder()
      .master("local[1]")
      .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "true")
      .withExtensions { ext =>
        ext.injectPlannerStrategy(_ => KylinSourceStrategy)
        ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        ext.injectPlannerStrategy(_ => new KylinDeltaSourceStrategy)
      }
      .getOrCreate()

    withTempPath { path =>
      val tempDir = path.getCanonicalPath

      val df = createSimpleFilePrunnerDF(spark, tempDir)
      assert(getFileSourceScanExec(df).isInstanceOf[KylinFileSourceScanExec])
      assert(getFileSourceScanExec(df).asInstanceOf[KylinFileSourceScanExec].inputRDD.isInstanceOf[CacheFileScanRDD])

    }

    withTempPath { path =>
      val tempDir = path.getCanonicalPath

      val df = createSimpleFileDeltaDF(spark, tempDir)
      assert(getFileSourceScanExec(df).isInstanceOf[KylinStorageScanExec])
      assert(getFileSourceScanExec(df).asInstanceOf[KylinStorageScanExec].inputRDD.isInstanceOf[CacheFileScanRDD])
    }

    spark.sparkContext.stop()
  }

  test("[Gluten] Create sharding read RDD with Soft affinity - CacheFileScanRDD") {
    val chLibPath = System.getProperty("clickhouse.lib.path")
    if (StringUtils.isEmpty(chLibPath) || !new File(chLibPath).exists) {
      log.warn("-Dclickhouse.lib.path is not set or path not exists, skip gluten config")
    } else {
      SparkSession.cleanupAnyExistingSession()
      val spark = SparkSession.builder()
        .master("local[1]")
        .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.io.compression.codec", "LZ4")
        .config("spark.gluten.sql.columnar.libpath", chLibPath)
        .config("spark.gluten.sql.enable.native.validation", "false")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2G")
        .config("spark.gluten.sql.columnar.extended.columnar.pre.rules",
          "org.apache.spark.sql.execution.gluten.ConvertKylinFileSourceToGlutenRule")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
          ext.injectPlannerStrategy(_ => new KylinDeltaSourceStrategy)
        }
        .getOrCreate()

      withTempPath { path =>
        val tempDir = path.getCanonicalPath
        val df = createSimpleFileDeltaDF(spark, tempDir)
        val transformed = HeuristicTransform.static()(df.queryExecution.executedPlan)
        val res = transformed.collect {
          case p: KylinStorageScanExecTransformer => p
        }
        assertResult(1)(res.size)
      }

      spark.sparkContext.stop()
    }
  }

  test("Create sharding read RDD without Soft affinity - FileScanRDD") {
    withTempPath { path =>
      SparkSession.cleanupAnyExistingSession()
      val tempDir = path.getCanonicalPath
      val spark = SparkSession.builder()
        .master("local[1]")
        .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "false")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        }
        .getOrCreate()

      val df = createSimpleFilePrunnerDF(spark, tempDir)

      assert(getFileSourceScanExec(df).asInstanceOf[KylinFileSourceScanExec].inputRDD.isInstanceOf[FileScanRDD])
      spark.sparkContext.stop()
    }
  }

  test("Create sharding read RDD with Soft affinity and Local cache - legacy in stream") {
    withTempPath { path =>
      SparkSession.cleanupAnyExistingSession()
      val tempDir = path.getCanonicalPath
      val spark = SparkSession.builder()
        .master("local[1]")
        .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "true")
        .config("spark.hadoop.spark.kylin.soft-affinity.enabled", "true")
        .config("spark.hadoop.fs.file.impl", "org.apache.kylin.cache.kylin.OnlyForTestCacheFileSystem")
        .config("fs.file.impl.disable.cache", "true")
        .config("spark.extraListeners", "org.apache.kylin.softaffinity.scheduler.SoftAffinityListener")
        .config("spark.hadoop.spark.kylin.local-cache.enabled", "true")
        .config("spark.hadoop.spark.kylin.local-cache.use.legacy.file-input-stream", "true")
        .config("spark.hadoop.spark.kylin.local-cache.use.buffer.file-input-stream", "false")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        }
        .getOrCreate()

      val df = createSimpleDF(spark, tempDir)
      checkAnswer(df, Seq(Row(0, 6), Row(1, 4), Row(2, 10)))
      spark.sparkContext.stop()
    }
  }

  test("Create sharding read RDD with Soft affinity and Local cache - buffer in stream") {
    withTempPath { path =>
      SparkSession.cleanupAnyExistingSession()
      val tempDir = path.getCanonicalPath
      val spark = SparkSession.builder()
        .master("local[1]")
        .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "true")
        .config("spark.hadoop.spark.kylin.soft-affinity.enabled", "true")
        .config("spark.hadoop.fs.file.impl", "org.apache.kylin.cache.kylin.OnlyForTestCacheFileSystem")
        .config("fs.file.impl.disable.cache", "true")
        .config("spark.extraListeners", "org.apache.kylin.softaffinity.scheduler.SoftAffinityListener")
        .config("spark.hadoop.spark.kylin.local-cache.enabled", "true")
        .config("spark.hadoop.spark.kylin.local-cache.use.legacy.file-input-stream", "false")
        .config("spark.hadoop.spark.kylin.local-cache.use.buffer.file-input-stream", "true")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        }
        .getOrCreate()

      val df = createSimpleDF(spark, tempDir)
      checkAnswer(df, Seq(Row(0, 6), Row(1, 4), Row(2, 10)))
      spark.sparkContext.stop()
    }
  }

  private def createSimpleFilePrunnerDF(spark: SparkSession, tempDir: String) = {
    val df = createSimpleDF(spark, tempDir)
    val plan = df.queryExecution.logical
    val fp = Mockito.mock(classOf[FilePruner])
    Mockito.when(fp.listFilesInternal(ArgumentMatchers.any(),
      ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(Seq.empty[PartitionDirectory])
    Mockito.when(fp.metadataOpsTimeNs).thenReturn(Some(0L))
    Mockito.when(fp.rootPaths).thenReturn(Seq.empty[Path])
    Dataset.ofRows(spark, replaceFileIndex(plan, fp))
  }

  private def createSimpleFileDeltaDF(spark: SparkSession, tempDir: String) = {
    val df = createSimpleDF(spark, tempDir)
    val plan = df.queryExecution.logical
    val fp = Mockito.mock(classOf[KylinDeltaLogFileIndex])
    Mockito.when(fp.listFiles(ArgumentMatchers.any(),
      ArgumentMatchers.any())).thenReturn(Seq.empty[PartitionDirectory])
    Mockito.when(fp.metadataOpsTimeNs).thenReturn(Some(0L))
    Mockito.when(fp.rootPaths).thenReturn(Seq.empty[Path])
    Mockito.when(fp.DeltaExpressionCache).thenReturn(CacheBuilder.newBuilder()
      .expireAfterAccess(12, TimeUnit.HOURS).build[(Seq[Expression], Seq[Expression]), (Seq[AddFile], Long)]())
    Dataset.ofRows(spark, replaceFileIndex(plan, fp))
  }

  private def createSimpleDF(spark: SparkSession, tempDir: String) = {
    spark.range(10)
      .selectExpr("id % 2 as a", "id % 3 as b", "id as c")
      .write
      .parquet(tempDir)

    spark.read.parquet(tempDir)
      .where("a = 0")
      .groupBy("b")
      .agg("c" -> "sum")
  }

  def replaceFileIndex(
                        target: LogicalPlan,
                        fileIndex: FileIndex): LogicalPlan = {
    target transform {
      case l@LogicalRelation(hfsr: HadoopFsRelation, _, _, _) =>
        l.copy(relation = hfsr.copy(location = fileIndex)(hfsr.sparkSession))
    }
  }

  private def getFileSourceScanExec(df: DataFrame) = {
    collectFirst(df.queryExecution.executedPlan) {
      case p: KylinFileSourceScanExec => p
      case p: KylinStorageScanExec => p
      case p: LayoutFileSourceScanExec => p
    }.get
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
