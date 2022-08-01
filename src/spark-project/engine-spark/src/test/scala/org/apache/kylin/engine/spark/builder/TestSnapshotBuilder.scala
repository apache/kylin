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

import java.util.concurrent.TimeoutException
import org.apache.kylin.metadata.model.{NDataModel, NDataModelManager, NTableMetadataManager}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.spark.SparkException
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

class TestSnapshotBuilder extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"

  // this model contain 7 dim table
  private val MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"

  override val master = "local[1]"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("snapshot -- check snapshot reuse") {
    val dataModel = getModel(MODEL_ID)

    overwriteSystemProp("kylin.snapshot.parallel-build-enabled", "false")
    buildSnapshot(dataModel, isMock = false, 1, null)
    buildSnapshot(dataModel, isMock = false, 1, null)
    buildSnapshot(dataModel, isMock = true, 2, null)
    buildSnapshot(dataModel, isMock = true, 2, null)
  }

  test("test concurrent snapshot success") {
    overwriteSystemProp("kylin.snapshot.parallel-build-enabled", "true")
    buildSnapshot(getModel(MODEL_ID))
  }

  test("test concurrent snapshot with timeout") {
    overwriteSystemProp("kylin.snapshot.parallel-build-timeout-seconds", "0")
    try {
      buildSnapshot(getModel(MODEL_ID))
      Assert.fail("build successfully, but this test should throw TimeoutException")
    } catch {
      case _: TimeoutException =>
      case e =>
        e.printStackTrace()
        Assert.fail(s"This test should throw TimeoutException")
    }
  }

  test("test concurrent snapshot with build error") {
    spark.stop()
    try {
      buildSnapshot(getModel(MODEL_ID))
      Assert.fail("This test should throw SparkException")
    } catch {
      case _: SparkException =>
      case e => Assert.fail(s"This test should throw SparkException, but it is ${e.getStackTrace.mkString("\n")}")
    }
    super.beforeAll()
  }


  private def buildSnapshot(dm: NDataModel): Unit = {
    buildSnapshot(dm, false, 1, null)
  }

  private def buildSnapshot(dm: NDataModel, isMock: Boolean, expectedSize: Int, ignoredSnapshotTables: java.util.Set[String]): Unit = {
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + dm.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem

    var snapshotBuilder = new SnapshotBuilder()
    if (isMock) {
      snapshotBuilder = new MockSnapshotBuilder()
    }

    // using getModel not dm is due to snapshot building do metaupdate
    snapshotBuilder.buildSnapshot(spark, getModel(dm.getId), ignoredSnapshotTables)
    Assert.assertEquals(snapshotBuilder.distinctTableDesc(dm, ignoredSnapshotTables).size, 7)

    val statuses = fs.listStatus(new Path(snapPath))
    for (fst <- statuses) {
      val list = fs.listStatus(fst.getPath)
      Assert.assertEquals(expectedSize, list.size)
    }

    val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, dm.getProject)
    for (table <- snapshotBuilder.distinctTableDesc(dm, ignoredSnapshotTables)) {
      val tableMeta = tableMetadataManager.getTableDesc(table.getIdentity)
      Assert.assertNotNull(tableMeta.getLastSnapshotPath)
      Assert.assertNotEquals(tableMeta.getLastSnapshotSize, 0)
      Assert.assertNotEquals(tableMeta.getSnapshotLastModified, 0)
    }
  }


  def getModel(modelId: String): NDataModel = {
    NDataModelManager.getInstance(getTestConfig, DEFAULT_PROJECT).getDataModelDesc(modelId)
  }

  def clearSnapshot(project: String) {
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + project + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clearSnapshot(DEFAULT_PROJECT)
  }

}
