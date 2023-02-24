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

package org.apache.kylin.engine.spark.job.stage.build.partition

import org.apache.kylin.common.util.RandomUtil
import org.apache.kylin.common.util.TestUtils.getTestConfig
import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.metadata.cube.model.{NDataflow, NDataflowManager, NDataflowUpdate}
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.junit.jupiter.api.Assertions.{assertNotNull, assertNull}
import org.mockito.Mockito
import org.scalatest.PrivateMethodTester

import scala.collection.JavaConverters._

class PartitionFlatTableAndDictBaseTest extends SparderBaseFunSuite with PrivateMethodTester
  with AdaptiveSparkPlanHelper with SharedSparkSession with LocalMetadata {
  val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6"
  val project = "default"

  test("test materializedFactTableView") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, project)
    val df: NDataflow = dsMgr.getDataflow(modelId)
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    val seg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val segmentJob = Mockito.mock(classOf[SegmentJob])
    Mockito.when(segmentJob.getConfig).thenReturn(getTestConfig)
    Mockito.when(segmentJob.getSparkSession).thenReturn(spark)
    Mockito.when(segmentJob.getJobId).thenReturn(RandomUtil.randomUUIDStr())
    Mockito.when(segmentJob.isPartialBuild).thenReturn(true)
    val buildParam = new BuildParam()
    val exec = new PartitionMaterializedFactTableView(segmentJob, seg, buildParam)
    exec.materializedFactTableView()
    assertNull(buildParam.getFlatTableDesc)
    assertNotNull(buildParam.getTableDesc)
    assertNull(buildParam.getSpanningTree)
    assertNotNull(buildParam.getPartitionSpanningTree)
    assertNotNull(buildParam.getFactTableDS)
    assertNotNull(buildParam.getFastFactTableDS)
  }

  test("test materializedFactTableView with isPartialBuild{false}") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, project)
    val df: NDataflow = dsMgr.getDataflow(modelId)
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    val seg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val segmentJob = Mockito.mock(classOf[SegmentJob])
    Mockito.when(segmentJob.getConfig).thenReturn(getTestConfig)
    Mockito.when(segmentJob.getSparkSession).thenReturn(spark)
    Mockito.when(segmentJob.getJobId).thenReturn(RandomUtil.randomUUIDStr())
    Mockito.when(segmentJob.isPartialBuild).thenReturn(false)
    val buildParam = new BuildParam()
    val exec = new PartitionMaterializedFactTableView(segmentJob, seg, buildParam)

    exec.materializedFactTableView()
    assertNull(buildParam.getFlatTableDesc)
    assertNotNull(buildParam.getTableDesc)
    assertNull(buildParam.getSpanningTree)
    assertNotNull(buildParam.getPartitionSpanningTree)
    assertNotNull(buildParam.getFactTableDS)
    assertNotNull(buildParam.getFastFactTableDS)
  }
}
