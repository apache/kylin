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

import java.util

import com.google.common.collect.Lists.newArrayList
import com.google.common.collect.{Lists, Sets}
import io.kyligence.kap.engine.spark.builder.DFBuilderHelper.ENCODE_SUFFIX
import io.kyligence.kap.engine.spark.job.{CuboidAggregator, UdfManager}
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager.NIndexPlanUpdater
import io.kyligence.kap.metadata.cube.model._
import io.kyligence.kap.metadata.model.NDataModel.Measure
import io.kyligence.kap.metadata.model.{ManagementType, NDataModel, NDataModelManager}
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.{FunctionDesc, ParameterDesc, SegmentRange, TblColRef}
import org.apache.spark.dict.{NGlobalDictBuilderAssist, NGlobalDictionaryV2}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert

import scala.collection.JavaConverters._

// scalastyle:off
class TestDFChooser extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"
  private val MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"
  private val CUBE_ID1 = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"
  private val CUBE_ID2 = "741ca86a-1f13-46da-a59f-95fb68615e3a"

  private val COLUMN_INDEX_BITMAP = 6 // DEFAULT.TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("[INDEX_BUILD] - global dict reuse") {
    UdfManager.create(spark)
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(CUBE_ID1)
    var indexMgr: NIndexPlanManager = NIndexPlanManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val dfCopy = df.copy()
    checkFlatTableEncoding(dfCopy.getUuid, dfCopy.getLastSegment, 0)

    var modelMgr: NDataModelManager = NDataModelManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    var model: NDataModel = modelMgr.getDataModelDesc(MODEL_ID)
    model.getAllMeasures.add(addBitmapMea(model))

    val modelUpdate: NDataModel = modelMgr.copyForWrite(model)
    modelUpdate.setManagementType(ManagementType.MODEL_BASED)
    modelMgr.updateDataModelDesc(modelUpdate)
    modelMgr = NDataModelManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    model = modelMgr.getDataModelDesc(MODEL_ID)

    indexMgr = NIndexPlanManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    indexMgr.updateIndexPlan(CUBE_ID1, new NIndexPlanUpdater {
      override def modify(copyForWrite: IndexPlan): Unit = {
        val indexEntity = copyForWrite.getAllIndexes.get(0)
        indexEntity.setId(111000)
        indexEntity.setMeasures(model.getEffectiveMeasureMap.inverse.values.asList)
        val layout = indexEntity.getLayouts.get(0)
        layout.setId(layout.getId + 1)
        val colList = newArrayList[Integer](indexEntity.getDimensions)
        colList.addAll(indexEntity.getMeasures)
        layout.setColOrder(colList)
        val cuboidsList = copyForWrite.getAllIndexes
        cuboidsList.add(indexEntity)
        copyForWrite.setIndexes(cuboidsList)
      }
    })

    checkFlatTableEncoding(dfCopy.getUuid, dfCopy.getLastSegment, 1)
  }

  test("[INDEX_BUILD] - Check if the number of columns in the encode matches the number of columns in agg") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(CUBE_ID1)
    val indexMgr: NIndexPlanManager = NIndexPlanManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val dfCopy = df.copy()

    val indexPlan = indexMgr.getIndexPlan(CUBE_ID1)
    val segment = dfCopy.getLastSegment

    // Check for the presence of the encode column
    checkEncodeAndAggIsMatch(segment, indexPlan, 0)

    // Check for the absence of an encode column
    checkEncodeAndAggIsMatch(segment, indexPlan, 1)
  }

  private def checkEncodeAndAggIsMatch(segment: NDataSegment, indexPlan: IndexPlan, indexId: Int): Unit = {
    val indexEntity = segment.getIndexPlan.getAllIndexes.get(indexId)
    val nSpanningTree = NSpanningTreeFactory.fromLayouts(indexEntity.getLayouts, MODEL_ID)
    val flatTableEncodeSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(segment, nSpanningTree)

    val meas = indexEntity.getEffectiveMeasures

    val flatTableAggSet = new util.HashSet[TblColRef]()
    for (mea <- meas.values().asScala) {
      if (DictionaryBuilderHelper.needGlobalDict(mea) != null) {
        val col = mea.getFunction.getParameters.get(0).getColRef
        flatTableAggSet.add(col)
      }
    }
    Assert.assertEquals(flatTableEncodeSet.size(), flatTableAggSet.size())
  }

  test("[INC_BUILD] - check df chooser and cuboid agg") {
    UdfManager.create(spark)
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    Assert.assertTrue(getTestConfig.getHdfsWorkingDirectory.startsWith("file:"))
    val df: NDataflow = dsMgr.getDataflow(CUBE_ID2)
    val dfCopy: NDataflow = df.copy()
    // cleanup all segments first
    val update = new NDataflowUpdate(dfCopy.getUuid)
    for (seg <- dfCopy.getSegments.asScala) {
      update.setToRemoveSegs(seg)
    }
    dsMgr.updateDataflow(update)
    val seg1 = dsMgr.appendSegment(dfCopy, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    checkCuboidAgg(checkFlatTableEncoding(dfCopy.getUuid, seg1, 1), seg1)
    val seg2 = dsMgr.appendSegment(dfCopy, new SegmentRange.TimePartitionedSegmentRange(1356019200000L, 1376019200000L))
    checkCuboidAgg(checkFlatTableEncoding(dfCopy.getUuid, seg2, 1), seg2)
    val seg3 = dsMgr.appendSegment(dfCopy, new SegmentRange.TimePartitionedSegmentRange(1376019200000L, 1496019200000L))
    checkCuboidAgg(checkFlatTableEncoding(dfCopy.getUuid, seg3, 1), seg3)
  }

  // Check that the number of columns generated by flattable is equal to the number of dims plus the number of encodings required.
  private def checkFlatTableEncoding(dfName: String, seg: NDataSegment, expectColSize: Int): Dataset[Row] = {
    val dsMgr = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df = dsMgr.getDataflow(dfName)
    val nSpanningTree = NSpanningTreeFactory.fromLayouts(seg.getIndexPlan.getAllLayouts, dfName)
    val dictColSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(seg, nSpanningTree)
    Assert.assertEquals(expectColSize, dictColSet.size())

    val flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan, seg.getSegRange, true)
    val encodeColSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, nSpanningTree)
    val flatTable = new CreateFlatTable(flatTableDesc, seg, nSpanningTree, spark, null)
    val afterJoin = flatTable.generateDataset(true)
    dictColSet.asScala.foreach(
      col => {
        val dict1 = new NGlobalDictionaryV2(seg.getProject, col.getTable, col.getName, seg.getConfig.getHdfsWorkingDirectory)
        val meta1 = dict1.getMetaInfo
        val needResizeBucketSize = dict1.getBucketSizeOrDefault(seg.getConfig.getGlobalDictV2MinHashPartitions) + 10
        NGlobalDictBuilderAssist.resize(col, seg, needResizeBucketSize, spark)
        val dict2 = new NGlobalDictionaryV2(seg.getProject, col.getTable, col.getName, seg.getConfig.getHdfsWorkingDirectory)
        Assert.assertEquals(meta1.getDictCount, dict2.getMetaInfo.getDictCount)
        Assert.assertEquals(meta1.getBucketSize + 10, dict2.getMetaInfo.getBucketSize)
      }
    )
    val dictSize = afterJoin.schema.count(_.name.endsWith(ENCODE_SUFFIX))
    Assert.assertEquals(dictSize, encodeColSet.size())
    afterJoin
  }

  // check cuboid agg choose need to encode column
  private def checkCuboidAgg(afterEncode: Dataset[Row], segment: NDataSegment): Unit = {
    for (layout <- segment.getIndexPlan.getAllLayouts.asScala) {
      if (layout.getId < IndexEntity.TABLE_INDEX_START_ID) {
        val dimIndexes = layout.getOrderedDimensions.keySet
        val measures = layout.getOrderedMeasures
        val nSpanningTree = NSpanningTreeFactory.fromLayouts(segment.getIndexPlan.getAllLayouts, MODEL_ID)
        val afterAgg = CuboidAggregator.agg(spark, afterEncode, dimIndexes, measures, segment, nSpanningTree)
        val aggExp = afterAgg.queryExecution.logical.children.head.output
        val colRefSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(segment, nSpanningTree)
        val needDictColIdSet = Sets.newHashSet[Integer]()
        for (col <- colRefSet.asScala) {
          needDictColIdSet.add(segment.getDataflow.getIndexPlan.getModel.getColumnIdByColumnName(col.getIdentity))
        }

        var encodedColNum = 0
        for (agg <- aggExp) {
          val aggName = agg.name
          if (aggName.endsWith(ENCODE_SUFFIX)) {
            encodedColNum = encodedColNum + 1
            val encodeColId = StringUtils.remove(aggName, ENCODE_SUFFIX)
            Assert.assertTrue(needDictColIdSet.contains(Integer.parseInt(encodeColId)))
          }
        }
        Assert.assertEquals(needDictColIdSet.size(), encodedColNum)
      }
    }
  }

  private def addBitmapMea(model: NDataModel): Measure = {
    val columnList = model.getEffectiveColsMap
    val measure = new NDataModel.Measure
    measure.setName("test_bitmap_add")
    val func = FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT_DISTINCT,
      Lists.newArrayList(ParameterDesc.newInstance(columnList.get(COLUMN_INDEX_BITMAP))), "bitmap")
    measure.setFunction(func)
    measure.setId(111000)
    measure
  }
}
