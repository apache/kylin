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
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataflow, NDataflowManager}
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

  // CUBE_PLANNER_TEST
  private val DF_NAME = "0f896779-a693-139f-6c50-895e240b4c97"

  val defaultTimeZone: TimeZone = TimeZone.getDefault


  override def beforeAll(): Unit = {
    super.beforeAll()
    // enable cube planner
    overwriteSystemProp("kylin.index.costbased.enabled", "true")
    val timeZoneStr = "GMT+0"
    TimeZone.setDefault(TimeZone.getTimeZone(timeZoneStr))
    logInfo(s"Current time zone set to $timeZoneStr")
    // load the model metadata
    NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, DEFAULT_PROJECT)
      .updateDataflow(DF_NAME, Updater(RealizationStatusEnum.OFFLINE))

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

  // test case: check the number of index for this model
  // the number of index for this model will be less than 100 after running the cube planner
  test("cube planner") {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(DF_NAME)
    val allCuboidLayouts: util.List[LayoutEntity] = df.getIndexPlan.getAllLayouts
    val count = allCuboidLayouts.size()
    // assert the number of index for this model
    // the original number is more than 1024: the aggregate group has 11 dimension
    assert(count < 100)
  }
}