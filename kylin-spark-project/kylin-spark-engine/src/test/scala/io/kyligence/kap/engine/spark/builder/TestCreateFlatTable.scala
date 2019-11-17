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
package io.kyligence.kap.engine.spark.builder

import java.text.SimpleDateFormat
import java.util.{TimeZone, UUID}

import io.kyligence.kap.engine.spark.builder.DFBuilderHelper.ENCODE_SUFFIX
import io.kyligence.kap.engine.spark.job.DFChooser
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory
import io.kyligence.kap.metadata.cube.model._
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.spark.InfoHelper
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert

import scala.collection.JavaConverters._

// scalastyle:off
class TestCreateFlatTable extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"
  private val MODEL_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"
  private val MODEL_NAME2 = "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94"

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  dateFormat.setTimeZone(TimeZone.getDefault)

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
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

  test("Check the flattable spark jobs num correctness") {
    val helper: InfoHelper = new InfoHelper(spark)

    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(MODEL_NAME2)
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    val groupId = UUID.randomUUID().toString
    spark.sparkContext.setJobGroup(groupId, "test", false)
    val seg1 = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val afterJoin1 = generateFlatTable(seg1, df, true)
    afterJoin1.collect()

    val jobs = helper.getJobsByGroupId(groupId)
    Assert.assertEquals(jobs.length, 1)
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
    val globalDictSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, toBuildTree)
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
