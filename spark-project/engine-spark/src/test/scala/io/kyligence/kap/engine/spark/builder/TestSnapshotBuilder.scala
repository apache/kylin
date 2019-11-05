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

import java.util.concurrent.{Callable, Executors, ExecutorService, Future, TimeoutException}

import com.google.common.collect.{Lists, Maps, Sets}
import io.kyligence.kap.metadata.cube.model.{NDataflow, NDataflowManager, NDataSegment}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.SparkException
import org.junit.Assert

import scala.collection.JavaConverters._

class TestSnapshotBuilder extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"

  private val DF_NAME = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"

  private val DF_NAME_SEQ = Seq(
    "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
    "741ca86a-1f13-46da-a59f-95fb68615e3a",
    "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96")

  override val master = "local[1]"
  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("snapshot -- check snapshot reuse") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(DF_NAME)
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + df.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    df.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-enabled", "false"))

    buildSnapshot(df, isMock = false, 1)
    buildSnapshot(df, isMock = false, 1)
    buildSnapshot(df, isMock = true, 2)
    buildSnapshot(df, isMock = true, 2)
    df.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-enabled", "true"))
  }

  test("snapshot -- check snapshot concurrent construction") {
    var dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + DEFAULT_PROJECT + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    roundTestBuildSnap()
  }

  test("test concurrent snapshot success") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + DEFAULT_PROJECT + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val df: NDataflow = dsMgr.getDataflow(DF_NAME)
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    buildSnapshotParallel(df)
    fs.delete(new Path(snapPath), true)
  }

  test("test concurrent snapshot with build error") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + DEFAULT_PROJECT + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val df: NDataflow = dsMgr.getDataflow(DF_NAME)
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    spark.stop()
    try {
      buildSnapshotParallel(df)
      Assert.fail("This test should throw SparkException")
    } catch {
      case _: SparkException =>
      case e => Assert.fail(s"This test should throw SparkException, but it is ${e.getStackTrace.mkString("\n")}")
    }
    super.initSpark()
    fs.delete(new Path(snapPath), true)
  }

  test("test concurrent snapshot with timeout") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + DEFAULT_PROJECT + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val df: NDataflow = dsMgr.getDataflow(DF_NAME)
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    df.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-timeout-seconds", "1"))
    try {
      buildSnapshotParallel(df)
      Assert.fail("This test should throw TimeoutException")
    } catch {
      case _: TimeoutException =>
      case e =>
        e.printStackTrace()
        Assert.fail(s"This test should throw TimeoutException")
    }
    df.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-timeout-seconds", "3600"))
    fs.delete(new Path(snapPath), true)
  }

  private def roundTestBuildSnap(): Unit = {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(10)
    try {
      val futureList = Lists.newArrayList[Future[NDataSegment]]()
      for (dfName <- DF_NAME_SEQ) {
        futureList.add(threadPool.submit(new BuildSnapshotThread(dfName)))
      }

      var isBuilding = true
      while (isBuilding) {
        if (futureList.asScala.filter(!_.isDone).size == 0) {
          isBuilding = false
        }
      }

      val snapSet = Sets.newHashSet[String]()
      var snapCount = 0
      for (future <- futureList.asScala) {
        snapSet.addAll(future.get().getSnapshots.values())
        snapCount = snapCount + future.get().getSnapshots.size()
      }

      Assert.assertTrue((21 > snapSet.size()) && (snapSet.size() >= 7))

    } finally {
      threadPool.shutdown()
    }
  }

  class BuildSnapshotThread(dfName: String) extends Callable[NDataSegment] {
    override def call(): NDataSegment = {
      var dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
      var df = dsMgr.getDataflow(dfName)
      df.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-enabled", "false"))
      val seg = df.getFirstSegment
      val dfCopy = df.copy
      val segCopy = dfCopy.getSegment(seg.getId)
      segCopy.setSnapshots(Maps.newHashMap())
      var snapshotBuilder = new DFSnapshotBuilder(segCopy, spark)
      val snapshot = snapshotBuilder.buildSnapshot
      df.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-enabled", "true"))
      snapshot
    }
  }

  private def buildSnapshot(df: NDataflow, isMock: Boolean, expectedSize: Int): Unit = {
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + df.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem

    for (segment <- df.getSegments.asScala) {
      val dfCopy = segment.getDataflow.copy
      val segCopy = dfCopy.getSegment(segment.getId)
      segCopy.setSnapshots(Maps.newHashMap())
      var snapshotBuilder = new DFSnapshotBuilder(segCopy, spark)
      if (isMock) {
        snapshotBuilder = new MockDFSnapshotBuilder(segCopy, spark)
      }
      snapshotBuilder.buildSnapshot
      Assert.assertEquals(snapshotBuilder.distinctTableDesc(df.getModel).size, 7)
    }

    val statuses = fs.listStatus(new Path(snapPath))
    for (fst <- statuses) {
      val list = fs.listStatus(fst.getPath)
      Assert.assertEquals(expectedSize, list.size)
    }
  }

  private def buildSnapshotParallel(df: NDataflow): Unit = {
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + df.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    for (segment <- df.getSegments.asScala) {
      val dfCopy = segment.getDataflow.copy
      val segCopy = dfCopy.getSegment(segment.getId)
      segCopy.setSnapshots(Maps.newHashMap())
      var snapshotBuilder = new DFSnapshotBuilder(segCopy, spark)
      snapshotBuilder.buildSnapshot
    }
    val statuses = fs.listStatus(new Path(snapPath))
    Assert.assertEquals(statuses.size, 7)
  }
}
