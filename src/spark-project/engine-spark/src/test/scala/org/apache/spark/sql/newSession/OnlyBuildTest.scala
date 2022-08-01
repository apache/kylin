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
package org.apache.spark.sql.newSession

import com.google.common.collect.Sets
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.IndexDataConstructor
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataflow, NDataflowManager}
import org.apache.kylin.metadata.model.NDataModelManager.NDataModelUpdater
import org.apache.kylin.metadata.model.{NDataModel, NDataModelManager}
import org.apache.kylin.job.engine.JobEngineConfig
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.spark.sql.test.SQLTestUtils

import java.util

abstract class OnlyBuildTest extends SQLTestUtils with WithKylinExternalCatalog {

  val project = "file_pruning"
  protected override val ut_meta = "../kap-it/src/test/resources/ut_meta/file_pruning"
  val dfID = "8c670664-8d05-466a-802f-83c023b56c77"

  protected def storageType: Integer

  def setStorage(modelMgr: NDataModelManager, modelName: String): Unit = {
    case class Updater(storageType: Integer) extends NDataModelUpdater {
      override def modify(copyForWrite: NDataModel): Unit = copyForWrite.setStorageType(storageType)
    }
    if (storageType != 1) {
      modelMgr.updateDataModel(modelName, Updater(storageType))
    }
  }

  override def beforeAll(): Unit = {
    overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1")
    super.beforeAll()
    val scheduler = NDefaultScheduler.getInstance(project)
    scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv))
    assert(scheduler.hasStarted)
    setStorage(NDataModelManager.getInstance(kylinConf, project), dfID)
  }

  override def afterAll(): Unit = {
    NDefaultScheduler.destroyInstance()
    super.afterAll()
  }

  test("testNonExistTimeRange") {
    val start: Long = SegmentRange.dateToLong("2023-01-01 00:00:00")
    val end: Long = SegmentRange.dateToLong("2025-01-01 00:00:00")
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(kylinConf, project)
    val df: NDataflow = dsMgr.getDataflow(dfID)
    val layouts: util.List[LayoutEntity] = df.getIndexPlan.getAllLayouts
    new IndexDataConstructor(project).buildIndex(dfID,
      new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts), true)
  }
}
