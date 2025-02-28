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

package org.apache.kylin.engine.spark.builder

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.SegmentBuildJob
import org.apache.kylin.engine.spark.job.step.ParamPropagation
import org.apache.kylin.engine.spark.job.step.build.BuildDict
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.spark.SparkExecutorInfo
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.mockito.Mockito

import scala.collection.JavaConverters._


class TestBuildDictLockConflict extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  def getTestConfig: KylinConfig = {
    KylinConfig.getInstanceFromEnv
  }

  test("waitTillWorkerRegistered") {
    getTestConfig.setProperty("kylin.engine.persist-flattable-enabled", "false")
    getTestConfig.setProperty("kylin.engine.count.lookup-table-max-time", "0")
    getTestConfig.setProperty("kylin.source.record-source-usage-enabled", "false")

    val project1 = "default"
    val model1 = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96"

    val dfMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, project1)
    val df: NDataflow = dfMgr.getDataflow(model1)
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dfMgr.updateDataflow(update)

    val seg = dfMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val toBuildTree = new AdaptiveSpanningTree(getTestConfig, new AdaptiveTreeBuilder(seg, seg.getIndexPlan.getAllLayouts))
    val flatTableDesc = new SegmentFlatTableDesc(getTestConfig, seg, toBuildTree)

    val spiedSparkSession = Mockito.spy(spark)
    val spiedSparkContext = Mockito.spy(spark.sparkContext)
    val spiedTracker = Mockito.spy(spark.sparkContext.statusTracker)
    val spiedSegmentJob = Mockito.spy(classOf[SegmentBuildJob])
    Mockito.doAnswer(_ => spiedSparkSession).when(spiedSegmentJob).getSparkSession
    val params = new ParamPropagation()
    params.setSpanningTree(toBuildTree)
    params.setFlatTableDesc(flatTableDesc)

    val flatTable = new BuildDict(spiedSegmentJob, seg, params) {
      override def execute(): Unit = {
        // do nothing.
      }
    }
    Mockito.when(spiedSparkSession.sparkContext).thenReturn(spiedSparkContext)
    Mockito.when(spiedSparkContext.statusTracker).thenReturn(spiedTracker)
    Mockito.when(spiedTracker.getExecutorInfos)
      .thenReturn(Array.empty[SparkExecutorInfo])
      .thenCallRealMethod()
    flatTable.waitTillWorkerRegistered(timeout = 1L)

    Mockito.when(spiedTracker.getExecutorInfos)
      .thenThrow(new NullPointerException("Test NPE!!"))
      .thenCallRealMethod()
    flatTable.waitTillWorkerRegistered(timeout = 1L)
  }
}
