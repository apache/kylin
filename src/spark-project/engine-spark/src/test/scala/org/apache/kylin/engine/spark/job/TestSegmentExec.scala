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

package org.apache.kylin.engine.spark.job

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.TestUtils.getTestConfig
import org.apache.kylin.engine.spark.job.step.ParamPropagation
import org.apache.kylin.engine.spark.job.step.build.FlatTableStage.Statistics
import org.apache.kylin.engine.spark.job.step.build.partition.{PartitionMaterializeFlatTable, PartitionRefreshColumnBytes}
import org.apache.kylin.engine.spark.scheduler.JobRuntime
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap
import org.apache.kylin.metadata.cube.model.{NDataSegment, NDataflowManager, NIndexPlanManager}
import org.apache.kylin.metadata.model.NDataModel.{DataStorageType, Measure}
import org.apache.kylin.metadata.model.{NDataModel, TblColRef}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.tracker.AppStatusContext
import org.junit.Assert
import org.mockito.Mockito
import org.scalatest.PrivateMethodTester

import java.util.concurrent.TimeUnit

class TestSegmentExec extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata with PrivateMethodTester {

  val PROJECT = "default"
  val DATAFLOW_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"
  val SEGMENT_ID = "ef5e0663-feba-4ed2-b71c-21958122bbff"

  private val myexec = new SegmentExec {
    override protected val jobId: String = ""
    override protected val project: String = ""
    override protected val segmentId: String = ""
    override protected val dataflowId: String = ""
    override protected val config: KylinConfig = null
    override protected val sparkSession: SparkSession = null
    override protected val dataModel: NDataModel = null
    override protected val storageType: DataStorageType = DataStorageType.V1
    override protected val appStatusContext: AppStatusContext = null
    override protected val runtime: JobRuntime = null

    override protected def columnIdFunc(colRef: TblColRef): String = ""

    override protected val sparkSchedulerPool: String = ""
  }

  test("test handle failure") {
    val func1 = PrivateMethod[Unit]('handleFailure)
    // test null
    val param1 = null
    myexec invokePrivate func1(param1)

    // test none
    val param2 = None
    myexec invokePrivate func1(param2)

    // test failure
    val param3 = Some(new Exception("test failure 1"))
    assertThrows[Exception](myexec invokePrivate func1(param3))
  }

  test("test fail fast poll") {
    val func1 = PrivateMethod[Int]('failFastPoll)
    // test illegal argument
    assertThrows[AssertionError](myexec invokePrivate func1(0L, TimeUnit.SECONDS))
  }

  test("test calDimRange isDimensionRangeFilterEnabled false") {
    val config = getTestConfig
    config.setProperty("kylin.build.multi-partition-filter-enabled", "true")
    config.setProperty("kylin.storage.columnar.dimension-range-filter-enabled", "false")

    val jobContext = Mockito.mock(classOf[SegmentJob])
    val dataSegment = Mockito.mock(classOf[NDataSegment])
    val dataModel = Mockito.mock(classOf[NDataModel])
    val partitionFlatTable = Mockito.mock(classOf[PartitionMaterializeFlatTable])

    Mockito.when(jobContext.getConfig).thenReturn(config)
    Mockito.when(jobContext.getSparkSession).thenReturn(spark)
    Mockito.when(jobContext.getDataflowId).thenReturn(DATAFLOW_ID)

    val mgr = NIndexPlanManager.getInstance(getTestConfig, PROJECT)
    val cubeIndexPlan = mgr.getIndexPlanByModelAlias("nmodel_basic")
    val cachedPartitionStats: Map[Long, Statistics] = Map(1L -> Statistics(111, Map("column" -> 22)))
    val mapBuilder: ImmutableBiMap.Builder[Integer, Measure] = ImmutableBiMap.builder()
    cubeIndexPlan.getEffectiveMeasures.forEach((key, value) => mapBuilder.put(key, value))
    val dataflow = NDataflowManager.getInstance(config, PROJECT).getDataflow(DATAFLOW_ID)
    val mergedSegment = dataflow.getSegment(SEGMENT_ID)
    mergedSegment.setFlatTableReady(true)
    prepareFlatTableParquet(config)

    Mockito.when(dataSegment.getModel).thenReturn(dataModel)
    Mockito.when(dataSegment.getModel.getEffectiveMeasures).thenReturn(mapBuilder.build())
    Mockito.when(dataSegment.getProject).thenReturn(PROJECT)
    Mockito.when(dataSegment.getDataflow).thenReturn(dataflow)
    Mockito.when(dataSegment.getId).thenReturn(SEGMENT_ID)
    Mockito.when(dataSegment.getIndexPlan).thenReturn(cubeIndexPlan)

    val buildParam = new ParamPropagation
    buildParam.setCachedPartitionStats(cachedPartitionStats)
    buildParam.setPartitionFlatTable(partitionFlatTable)

    val partitionRefreshColumnBytes = new PartitionRefreshColumnBytes(jobContext, dataSegment, buildParam)
    Assert.assertEquals("PartitionRefreshColumnBytes", partitionRefreshColumnBytes.getStageName)
    try {
      partitionRefreshColumnBytes.execute()
    } catch {
      case ignore: Exception => ""
    }
  }

  test("test calDimRange isDimensionRangeFilterEnabled true") {
    val config = getTestConfig
    config.setProperty("kylin.build.multi-partition-filter-enabled", "true")

    val jobContext = Mockito.mock(classOf[SegmentJob])
    val dataSegment = Mockito.mock(classOf[NDataSegment])
    val dataModel = Mockito.mock(classOf[NDataModel])
    val partitionFlatTable = Mockito.mock(classOf[PartitionMaterializeFlatTable])

    Mockito.when(jobContext.getConfig).thenReturn(config)
    Mockito.when(jobContext.getSparkSession).thenReturn(spark)
    Mockito.when(jobContext.getDataflowId).thenReturn(DATAFLOW_ID)

    val mgr = NIndexPlanManager.getInstance(getTestConfig, PROJECT)
    val cubeIndexPlan = mgr.getIndexPlanByModelAlias("nmodel_basic")
    val cachedPartitionStats: Map[Long, Statistics] = Map(1L -> Statistics(111, Map("column" -> 22)))
    val mapBuilder: ImmutableBiMap.Builder[Integer, Measure] = ImmutableBiMap.builder()
    cubeIndexPlan.getEffectiveMeasures.forEach((key, value) => mapBuilder.put(key, value))
    val dataflow = NDataflowManager.getInstance(config, PROJECT).getDataflow(DATAFLOW_ID)
    val prepareSegment = dataflow.getSegment(SEGMENT_ID)
    prepareSegment.setFlatTableReady(true)
    prepareFlatTableParquet(config)

    Mockito.when(dataSegment.getModel).thenReturn(dataModel)
    Mockito.when(dataSegment.getModel.getEffectiveMeasures).thenReturn(mapBuilder.build())
    Mockito.when(dataSegment.getProject).thenReturn(PROJECT)
    Mockito.when(dataSegment.getDataflow).thenReturn(dataflow)
    Mockito.when(dataSegment.getId).thenReturn(SEGMENT_ID)
    Mockito.when(dataSegment.getIndexPlan).thenReturn(cubeIndexPlan)

    val buildParam = new ParamPropagation
    buildParam.setCachedPartitionStats(cachedPartitionStats)
    buildParam.setPartitionFlatTable(partitionFlatTable)

    val partitionRefreshColumnBytes = new PartitionRefreshColumnBytes(jobContext, dataSegment, buildParam)
    Assert.assertEquals("PartitionRefreshColumnBytes", partitionRefreshColumnBytes.getStageName)
    try {
      partitionRefreshColumnBytes.execute()
    } catch {
      case ignore: Exception => ""
    }
  }

  def prepareFlatTableParquet(config: KylinConfig): Unit = {
    val schema = StructType(List(
      StructField("col1", DataTypes.LongType, nullable = false),
      StructField("col2", DataTypes.LongType, nullable = false),
      StructField("col3", DataTypes.LongType, nullable = false),
      StructField("col4", DataTypes.StringType, nullable = false),
      StructField("col5", DataTypes.LongType, nullable = false)
    ))
    val rows = Seq(
      Row(1L, 5L, 1L, "Customer#000000001", 1L),
      Row(2L, 6L, 2L, "Customer#000000002", 2L)
    )
    val flatTableDS = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    flatTableDS.write.mode(SaveMode.Overwrite).parquet(config.getFlatTableDir(PROJECT, DATAFLOW_ID, SEGMENT_ID).toString)
  }
}
