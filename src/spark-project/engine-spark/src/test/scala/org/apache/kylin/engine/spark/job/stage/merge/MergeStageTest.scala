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

package org.apache.kylin.engine.spark.job.stage.merge

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.NDataModel
import org.apache.spark.sql.common.LocalMetadata
import org.junit.Assert
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite


class MergeStageTest extends AnyFunSuite with LocalMetadata {

  private val path = "./test"
  private val tempPath = path + "_temp"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteQuietly(new File(path))
    FileUtils.deleteQuietly(new File(tempPath))
  }

  class MergeStageMock(jobContext: SegmentJob, dataSegment: NDataSegment)
    extends MergeStage(jobContext, dataSegment) {
    override def execute(): Unit = {}

    override def getUnmergedFTPaths: Seq[Path] = super.getUnmergedFTPaths
  }

  def testGetUnmergedFTPaths(config: KylinConfig): Unit = {
    val jobContext = Mockito.mock(classOf[SegmentJob])
    Mockito.when(jobContext.getConfig).thenReturn(config)

    val dataSegment = Mockito.mock(classOf[NDataSegment])
    val dataModel = Mockito.mock(classOf[NDataModel])
    dataModel.setStorageType(0)
    Mockito.when(dataSegment.getModel).thenReturn(dataModel)

    val mergeStageMock = new MergeStageMock(jobContext, dataSegment)
    Assert.assertTrue(mergeStageMock.getUnmergedFTPaths.isEmpty)
    config.setProperty("kylin.env.hdfs-write-working-dir", "")
    config.setProperty("kylin.env.hdfs-write-working-dir", "")
  }

  test("getUnmergedFTPaths with getBuildConf empty getWritingClusterWorkingDir empty") {
    testGetUnmergedFTPaths(KylinConfig.getInstanceFromEnv)
  }

  test("getUnmergedFTPaths with getBuildConf empty getWritingClusterWorkingDir not empty") {
    val config = KylinConfig.getInstanceFromEnv
    config.setProperty("kylin.env.hdfs-write-working-dir", "file://")
    testGetUnmergedFTPaths(config)
  }

  test("getUnmergedFTPaths with getBuildConf not empty getWritingClusterWorkingDir empty") {
    val config = KylinConfig.getInstanceFromEnv
    config.setProperty("kylin.engine.submit-hadoop-conf-dir", "/kylin")
    testGetUnmergedFTPaths(config)
  }

  test("getUnmergedFTPaths with getBuildConf not empty getWritingClusterWorkingDir not empty") {
    val config = KylinConfig.getInstanceFromEnv
    config.setProperty("kylin.engine.submit-hadoop-conf-dir", "/kylin")
    config.setProperty("kylin.env.hdfs-write-working-dir", "file://abc")
    testGetUnmergedFTPaths(config)
  }

}
