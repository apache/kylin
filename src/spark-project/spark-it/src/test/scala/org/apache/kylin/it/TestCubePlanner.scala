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

package org.apache.kylin.it

import java.util
import java.util.TimeZone

import org.apache.kylin.common._
import org.apache.kylin.metadata.cube.model.NDataflowManager.NDataflowUpdater
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.realization.RealizationStatusEnum
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.utils.SchemaProcessor

class TestCubePlanner extends SparderBaseFunSuite
  with LocalMetadata
  with JobSupport
  with QuerySupport
  with CompareSupport
  with SSSource
  with AdaptiveSparkPlanHelper
  with Logging {

  override val DEFAULT_PROJECT = "default"

  override protected def getProject: String = DEFAULT_PROJECT

  private val DF_NAME = "d863b37c-e1a9-717f-7df7-74991815b1eb"

  val defaultTimeZone: TimeZone = TimeZone.getDefault

  private val modelIds = Seq(DF_NAME)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // enable cube planner
    val timeZoneStr = "GMT+0"
    TimeZone.setDefault(TimeZone.getTimeZone(timeZoneStr))
    logInfo(s"Current time zone set to $timeZoneStr")
    KylinConfig.getInstanceFromEnv.setProperty("kylin.index.costbased.enabled", "true")

    // load the model metadata
    addModels("src/test/resources/index_planner/", modelIds)
    // build one segment and get the recommended segment
    // before recommend, the model has more than 2048 index, after build the number of index will be less than 100
    build()
  }

  override def afterAll(): Unit = {
    NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, DEFAULT_PROJECT)
      .updateDataflow(DF_NAME, Updater(RealizationStatusEnum.ONLINE))
    super.afterAll()
    SparderEnv.cleanCompute()
    TimeZone.setDefault(defaultTimeZone)
  }

  case class Updater(status: RealizationStatusEnum) extends NDataflowUpdater {
    override def modify(copyForWrite: NDataflow): Unit = copyForWrite.setStatus(status)
  }

  def build(): Unit = {
    // build one segment
    buildOneSegementForCubePlanner(DF_NAME)
    // replace metadata with new one after build
    dumpMetadata()
    SchemaProcessor.checkSchema(spark, DF_NAME, DEFAULT_PROJECT)
    checkOrder(spark, DF_NAME, DEFAULT_PROJECT)
  }

  def rebuild(): Unit = {
    // build one segment
    buildOneSegementForCubePlanner(DF_NAME)
    // replace metadata with new one after build
    dumpMetadata()
  }

  // test case: check the number of index for this model
  // the number of index for this model will be less than 100 after running the cube planner
  test("test cube planner basic") {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(DF_NAME)
    val allLayouts: util.List[LayoutEntity] = df.getIndexPlan.getAllLayouts
    val count = allLayouts.size()
    // assert the number of index for this model
    // the original number is more than 1024: the aggregate group has 11 dimension
    assert(count < 100)
  }

  // test corner case:
  // 1. build segment, get the recommended layouts `layouts1`
  // 2. delete one dimension for this model which will impact the aggregation group
  // 3. rebuild the segment, and will get the recommended layouts `layout2`
  // 4. layout1 != layout2
  test("test cube planner with changing the metadata") {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(DF_NAME)
    // layouts1
    val allLayouts1: util.List[LayoutEntity] = df.getIndexPlan.getAllLayouts
    val count1 = allLayouts1.size()

    // layout2
    // delete the one of the dimension
    // rebuild the segment
    val indexMgr: NIndexPlanManager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT)
    indexMgr.updateIndexPlan(DF_NAME, (copyForWrite: IndexPlan) => {
      val ruleIndex = copyForWrite.getRuleBasedIndex
      // edit the aggregation group
      val aggGroups = ruleIndex.getAggregationGroups
      // "includes" : [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ]
      // "mandatory_dims" : [ 0 ],
      val aggGroup = aggGroups.get(0)
      val newIncludes: Array[Integer] = Array(0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
      // set new includes: [ 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ]
      aggGroup.setIncludes(newIncludes)
      // set the new agg group to the rule base index
      ruleIndex.setAggregationGroups(aggGroups)
      ruleIndex.init()
      copyForWrite.setRuleBasedIndex(ruleIndex)
    })

    // after update the new index, we need to rebuild the segment
    rebuild()

    // check the result of new layouts for the new aggregation group
    val dsMgr2: NDataflowManager = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    val df2: NDataflow = dsMgr2.getDataflow(DF_NAME)
    // layouts2
    val allLayouts2: util.List[LayoutEntity] = df2.getIndexPlan.getAllLayouts
    val count2 = allLayouts2.size()

    // new layouts are not equal to the original layouts
    assert(count2 < 100)
    assert(count1 != count2)
  }

}