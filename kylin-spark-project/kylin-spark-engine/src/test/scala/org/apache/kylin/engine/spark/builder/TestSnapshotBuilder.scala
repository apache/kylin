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

import java.io.File
import java.util.concurrent._

import org.apache.kylin.shaded.com.google.common.collect.{Lists, Sets}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.cube.{CubeInstance, CubeManager}
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.metadata.{MetadataConverter, SegmentInfo}
import org.apache.spark.SparkException
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

import scala.collection.JavaConverters._

class TestSnapshotBuilder extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"

  private val CUBE_NAME = "ci_left_join_cube"

  private val CUBE_NAME_SEQ = Seq(
    "ci_left_join_cube")

  override val master = "local[1]"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("snapshot -- check snapshot reuse") {
    KylinBuildEnv.getOrCreate(getTestConfig)
    val cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
    val cube: CubeInstance = cubeMgr.getCube(CUBE_NAME)

    val snapPath = getTestConfig.getHdfsWorkingDirectory + cube.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    cube.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-enabled", "false"))

    buildSnapshot(cube, 1)
    buildSnapshot(cube, 1)
    cube.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-enabled", "true"))
  }

  test("snapshot -- check snapshot concurrent construction") {
    KylinBuildEnv.getOrCreate(getTestConfig)
    var cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
    val snapPath = getTestConfig.getHdfsWorkingDirectory + DEFAULT_PROJECT + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    roundTestBuildSnap()
  }

  test("test concurrent snapshot success") {
    KylinBuildEnv.getOrCreate(getTestConfig)
    val cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
    val snapPath = getTestConfig.getHdfsWorkingDirectory + DEFAULT_PROJECT + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val cube: CubeInstance = cubeMgr.getCube(CUBE_NAME)
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    buildSnapshotParallel(cube)
    fs.delete(new Path(snapPath), true)
  }

  test("test concurrent snapshot with build error") {
    KylinBuildEnv.getOrCreate(getTestConfig)
    val cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
    val snapPath = getTestConfig.getHdfsWorkingDirectory + DEFAULT_PROJECT + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val cube: CubeInstance = cubeMgr.getCube(CUBE_NAME)
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    spark.stop()
    try {
      buildSnapshotParallel(cube)
      Assert.fail("This test should throw SparkException")
    } catch {
      case _: SparkException =>
      case e => Assert.fail(s"This test should throw SparkException, but it is ${e.getStackTrace.mkString("\n")}")
    }
    super.initSpark()
    fs.delete(new Path(snapPath), true)
  }

  test("test concurrent snapshot with timeout") {
    KylinBuildEnv.getOrCreate(getTestConfig)
    val cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
    val snapPath = getTestConfig.getHdfsWorkingDirectory + DEFAULT_PROJECT + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val cube: CubeInstance = cubeMgr.getCube(CUBE_NAME)
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
    cube.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-timeout-seconds", "1"))
    try {
      buildSnapshotParallel(cube)
      Assert.fail("This test should throw TimeoutException")
    } catch {
      case _: TimeoutException =>
      case e =>
        e.printStackTrace()
        Assert.fail(s"This test should throw TimeoutException")
    }
    cube.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-timeout-seconds", "3600"))
    fs.delete(new Path(snapPath), true)
  }

  private def roundTestBuildSnap(): Unit = {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(10)
    try {
      val futureList = Lists.newArrayList[Future[SegmentInfo]]()
      for (cubeName <- CUBE_NAME_SEQ) {
        futureList.add(threadPool.submit(new BuildSnapshotThread(cubeName)))
      }

      var isBuilding = true
      while (isBuilding) {
        if (futureList.asScala.filter(!_.isDone).size == 0) {
          isBuilding = false
        }
      }

      val snapSet = Sets.newHashSet[String]()
      var snapCount = 0
      val cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
      for (future <- futureList.asScala) {
        val segmentInfo = future.get
        val segment = cubeMgr.getCube(CUBE_NAME).getSegmentById(segmentInfo.id)
        snapSet.addAll(segment.getSnapshots.values())
        snapCount = snapCount + segment.getSnapshots.size()
      }

      Assert.assertTrue(snapSet.size() >= 5)

    } finally {
      threadPool.shutdown()
    }
  }

  class BuildSnapshotThread(cubeName: String) extends Callable[SegmentInfo] {
    override def call(): SegmentInfo = {
      var cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
      var cube = cubeMgr.getCube(cubeName)
      cube.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-enabled", "false"))
      val seg = cube.getFirstSegment
      val cubeCopy = cube.latestCopyForWrite()
      val segCopy = cubeCopy.getSegmentById(seg.getUuid)
      segCopy.setSnapshots(new ConcurrentHashMap())

      var snapshotBuilder = new CubeSnapshotBuilder(
        MetadataConverter.getSegmentInfo(segCopy.getCubeInstance, segCopy.getUuid, segCopy.getName, segCopy.getStorageLocationIdentifier), spark)
      val snapshot = snapshotBuilder.buildSnapshot
      cube.getSegments.asScala.foreach(_.getConfig.setProperty("kylin.snapshot.parallel-build-enabled", "true"))
      snapshot
    }
  }

  private def buildSnapshot(cube: CubeInstance, expectedSize: Int): Unit = {
    val snapPath = getTestConfig.getHdfsWorkingDirectory + cube.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    for (segment <- cube.getSegments.asScala) {
      val cubeCopy = segment.getCubeInstance.latestCopyForWrite()
      val segCopy = cubeCopy.getSegmentById(segment.getUuid)
      segCopy.setSnapshots(new ConcurrentHashMap())
      val segInfo = MetadataConverter.getSegmentInfo(segCopy.getCubeInstance, segCopy.getUuid, segCopy.getName, segCopy.getStorageLocationIdentifier)
      var snapshotBuilder = new CubeSnapshotBuilder(segInfo, spark)
      snapshotBuilder.buildSnapshot
    }

    val statuses = fs.listStatus(new Path(snapPath))
    for (fst <- statuses) {
      val list = fs.listStatus(fst.getPath)
      Assert.assertEquals(expectedSize, list.size)
    }
  }

  private def buildSnapshotParallel(cube: CubeInstance): Unit = {
    val snapPath = getTestConfig.getHdfsWorkingDirectory + cube.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    for (segment <- cube.getSegments.asScala) {
      val cubeCopy = segment.getCubeInstance.latestCopyForWrite()
      val segCopy = cubeCopy.getSegmentById(segment.getUuid)
      segCopy.setSnapshots(new ConcurrentHashMap())
      var snapshotBuilder = new CubeSnapshotBuilder(
        MetadataConverter.getSegmentInfo(segCopy.getCubeInstance, segCopy.getUuid, segCopy.getName, segCopy.getStorageLocationIdentifier), spark)
      snapshotBuilder.buildSnapshot
    }
    val statuses = fs.listStatus(new Path(snapPath))
    Assert.assertEquals(statuses.size, 5)
  }
}
