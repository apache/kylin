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

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.builder.DFBuilderHelper.ENCODE_SUFFIX
import org.apache.kylin.engine.spark.job.DFChooser
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.{NDataModel, NDataModelManager, SegmentRange}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert

import scala.collection.JavaConverters._

// scalastyle:off
class TestCreateFlatTable extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"
  private val MODEL_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"
  private val MODEL_NAME2 = "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94"
  private val MODEL_NAME3 = "741ca86a-1f13-46da-a59f-95fb68615e3a"

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT))
  dateFormat.setTimeZone(TimeZone.getDefault)

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("Check the flattable with model filter") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(MODEL_NAME3)

    // set model filter
    val modelMgr = NDataModelManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val modelUpdate: NDataModel = modelMgr.copyForWrite(modelMgr.getDataModelDesc(MODEL_NAME3))
    modelUpdate.setFilterCondition("TEST_KYLIN_FACT.TRANS_ID <> 123")
    modelMgr.updateDataModelDesc(modelUpdate)

    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    // resource detect mode
    val seg1 = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val afterJoin1 = generateFlatTable(seg1, df, false)
    val logicalPlanStr = afterJoin1.queryExecution.logical.toString()
    // there should be some filter added to the query exec like
    // Filter ((1 = 1) && NOT (TEST_KYLIN_FACT_0_DOT_0_TRANS_ID#42L = cast(123 as bigint)))
    Assert.assertTrue(logicalPlanStr.contains("Filter ((1 = 1) AND NOT (TEST_KYLIN_FACT_0_DOT_0_TRANS_ID"))
    Assert.assertTrue(logicalPlanStr.contains("= cast(123 as bigint)"))
    checkFilterCondition(afterJoin1, seg1)
    checkEncodeCols(afterJoin1, seg1, false)
  }

  test("Check the flattable filter and encode") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(MODEL_NAME1)
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    // resource detect mode
    val seg1 = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val afterJoin1 = generateFlatTable(seg1, df, false)
    checkFilterCondition(afterJoin1, seg1)
    checkEncodeCols(afterJoin1, seg1, false)

    val seg2 = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1356019200000L, 1376019200000L))
    val afterJoin2 = generateFlatTable(seg2, df, false)
    checkFilterCondition(afterJoin2, seg2)
    checkEncodeCols(afterJoin2, seg2, false)

    // cubing mode
    val seg3 = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1376019200000L, 1396019200000L))
    val afterJoin3 = generateFlatTable(seg3, df, true)
    checkEncodeCols(afterJoin3, seg3, true)

    val seg4 = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1396019200000L, 1416019200000L))
    val afterJoin4 = generateFlatTable(seg4, df, true)
    checkEncodeCols(afterJoin4, seg4, true)
  }

  private def checkFilterCondition(ds: Dataset[Row], seg: NDataSegment) = {
    val queryExecution = ds.queryExecution.simpleString
    val startTime = dateFormat.format(seg.getSegRange.getStart)
    val endTime = dateFormat.format(seg.getSegRange.getStart)

    //Test Filter Condition
    Assert.assertTrue(queryExecution.contains(startTime))
    Assert.assertTrue(queryExecution.contains(endTime))
  }

  private def checkEncodeCols(ds: Dataset[Row], seg: NDataSegment, needEncode: Boolean) = {
    val toBuildTree = NSpanningTreeFactory.fromLayouts(seg.getIndexPlan.getAllLayouts, MODEL_NAME1)
    val globalDictSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, toBuildTree.getAllIndexEntities)
    val actualEncodeDictSize = ds.schema.count(_.name.endsWith(ENCODE_SUFFIX))
    if (needEncode) {
      Assert.assertEquals(globalDictSet.size(), actualEncodeDictSize)
    } else {
      Assert.assertEquals(0, actualEncodeDictSize)
    }
  }

  private def generateFlatTable(seg: NDataSegment, df: NDataflow, needEncode: Boolean): Dataset[Row] = {
    val toBuildTree = NSpanningTreeFactory.fromLayouts(seg.getIndexPlan.getAllLayouts, MODEL_NAME1)
    val needJoin = DFChooser.needJoinLookupTables(seg.getModel, toBuildTree)
    val flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan, seg.getSegRange, needJoin)
    val flatTable = new CreateFlatTable(flatTableDesc, seg, toBuildTree, spark, null)
    val afterJoin = flatTable.generateDataset(needEncode)
    afterJoin
  }
}
