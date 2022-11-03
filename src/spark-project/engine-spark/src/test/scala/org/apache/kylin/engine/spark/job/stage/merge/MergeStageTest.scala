/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
