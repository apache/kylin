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

package org.apache.kylin.common

import java.io.File
import java.util.Objects

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.persistence.ResourceStore
import org.apache.kylin.common.persistence.metadata.MetadataStore
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.common.util.{RandomUtil, Unsafe}
import org.apache.kylin.engine.spark.ExecutableUtils
import org.apache.kylin.engine.spark.job.{NSparkCubingJob, NSparkCubingStep, NSparkMergingJob, NSparkMergingStep}
import org.apache.kylin.engine.spark.merger.{AfterBuildResourceMerger, AfterMergeOrRefreshResourceMerger}
import org.apache.kylin.engine.spark.utils.{FileNames, HDFSUtils}
import org.apache.kylin.guava30.shaded.common.collect.{Lists, Maps, Sets}
import org.apache.kylin.job.execution.{AbstractExecutable, ExecutableManager, ExecutableState}
import org.apache.kylin.job.util.JobContextUtil
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.kylin.metadata.realization.RealizationStatusEnum
import org.apache.kylin.query.runtime.plan.TableScanPlan
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.common.SparderQueryTest
import org.apache.spark.sql.functions._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.collection.JavaConverters._

trait JobSupport
  extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging {
  self: Suite =>
  val DEFAULT_PROJECT = "default"
  val schedulerInterval = "1"
  // var scheduler: NDefaultScheduler = _
  val systemProp: java.util.HashMap[String, String] = Maps.newHashMap[String, String]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    Unsafe.setProperty("kylin.job.scheduler.poll-interval-second", schedulerInterval)
    JobContextUtil.cleanUp()
    JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv)
  }

  private def restoreSparderEnv(): Unit = {
    for (prop <- systemProp.keySet.asScala) {
      restoreIfNeed(prop)
    }
    systemProp.clear()
  }

  private def restoreIfNeed(prop: String): Unit = {
    val value = systemProp.get(prop)
    if (value == null) {
      Unsafe.clearProperty(prop)
    } else {
      Unsafe.setProperty(prop, value)
    }
  }

  override def afterAll(): Unit = {
    // scheduler.shutdown()
    restoreSparderEnv()
    super.afterAll()
    JobContextUtil.cleanUp()
  }

  @throws[Exception]
  def changeCubeStatus(dfName: String,
                       status: RealizationStatusEnum,
                       prj: String = DEFAULT_PROJECT): Unit = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, prj)
    val df: NDataflow = dsMgr.getDataflow(dfName)
    val update: NDataflowUpdate = new NDataflowUpdate(df.getUuid)
    update.setStatus(status)
    dsMgr.updateDataflow(update)
  }

  @throws[Exception]
  def buildCubes(dfsName: List[String]): Unit = {
    dfsName.foreach(dfName => fullBuildCube(dfName, DEFAULT_PROJECT))
  }

  @throws[Exception]
  protected def fullBuildCube(dfName: String,
                              prj: String = DEFAULT_PROJECT): Unit = {
    logInfo("Build cube :" + dfName)
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, prj)
    // ready dataflow, segment, cuboid layout
    var df: NDataflow = dsMgr.getDataflow(dfName)
    // cleanup all segments first
    val update: NDataflowUpdate = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    UnitOfWork.doInTransactionWithRetry(() => {
      NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, prj).updateDataflow(update)
    }, prj)

    df = dsMgr.getDataflow(dfName)
    val layouts: java.util.List[LayoutEntity] =
      df.getIndexPlan.getAllLayouts
    val round1: java.util.List[LayoutEntity] = Lists.newArrayList(layouts)
    buildCuboid(dfName,
      SegmentRange.TimePartitionedSegmentRange.createInfinite,
      Sets.newLinkedHashSet(round1),
      prj)
  }

  def checkSnapshotTable(dfId: String, id: String, project: String): Unit = {
    val conf = KylinConfig.getInstanceFromEnv
    val manager = NDataflowManager.getInstance(conf, project)
    val workingDirectory = KapConfig.wrap(conf).getMetadataWorkingDirectory

    val dataflow = manager.getDataflow(dfId)
    val model = dataflow.getModel;
    model.getLookupTables.asScala.foreach({
      table =>
        val parent = FileNames.snapshotFileWithWorkingDir(table.getTableDesc, workingDirectory)
        Assert.assertTrue(s"$parent should ony one snapshot file.",
          HDFSUtils.listSortedFileFrom(parent).size == 1)
    })
  }

  @throws[Exception]
  protected def buildSegment(dfUuid: String,
                             segmentRange: SegmentRange[_ <: Comparable[_]],
                             toBuildLayouts: java.util.Set[LayoutEntity],
                             prj: String): NDataSegment = {
    val oneSeg: NDataSegment = UnitOfWork.doInTransactionWithRetry(() => {
      val dsMgr: NDataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, prj)
      val df: NDataflow = dsMgr.getDataflow(dfUuid)
      // ready dataflow, segment, cuboid layout
      dsMgr.appendSegment(df, segmentRange)
    }, prj)

    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val execMgr: ExecutableManager = ExecutableManager.getInstance(config, prj)
    // create job, and the job type is `inc_build`
    val job: NSparkCubingJob = NSparkCubingJob.createIncBuildJob(Sets.newHashSet(oneSeg), toBuildLayouts, "ADMIN", null)
    val sparkStep: NSparkCubingStep = job.getSparkCubingStep
    val distMetaUrl: StorageURL = StorageURL.valueOf(sparkStep.getDistMetaUrl)
    Assert.assertEquals("hdfs", distMetaUrl.getScheme)
    Assert.assertTrue(
      distMetaUrl
        .getParameter("path")
        .startsWith(config.getHdfsWorkingDirectory))
    // launch the job
    execMgr.addJob(job)
    if (!Objects.equals(wait(job), ExecutableState.SUCCEED)) {
      val message = job.getTasks.asScala
        .find(executable => Objects.equals(executable.getStatus, ExecutableState.ERROR))
        .map(task => execMgr.getOutputFromHDFSByJobId(job.getId, task.getId, Integer.MAX_VALUE).getVerboseMsg)
        .getOrElse("Unknown Error")
      throw new IllegalStateException(message);
    }

    UnitOfWork.doInTransactionWithRetry(() => {
      val conf: KylinConfig = KylinConfig.getInstanceFromEnv
      val buildStore: ResourceStore = ExecutableUtils.getRemoteStore(conf, job.getSparkCubingStep)
      val merger: AfterBuildResourceMerger = new AfterBuildResourceMerger(conf, prj)
      val layoutIds: java.util.Set[java.lang.Long] = toBuildLayouts.asScala.map(c => new java.lang.Long(c.getId)).asJava
      merger.mergeAfterIncrement(dfUuid, oneSeg.getId, layoutIds, buildStore)
      checkSnapshotTable(dfUuid, oneSeg.getId, oneSeg.getProject)
    }, prj)
    oneSeg
  }

  @throws[Exception]
  protected def buildCuboid(cubeName: String,
                            segmentRange: SegmentRange[_ <: Comparable[_]],
                            toBuildLayouts: java.util.Set[LayoutEntity],
                            prj: String): NDataSegment = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, prj)
    val execMgr: ExecutableManager = ExecutableManager.getInstance(config, prj)
    val df: NDataflow = dsMgr.getDataflow(cubeName)
    // ready dataflow, segment, cuboid layout
    val oneSeg: NDataSegment = UnitOfWork.doInTransactionWithRetry(() => {
      NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, prj).appendSegment(df, segmentRange)
    }, prj);
    val job: NSparkCubingJob = NSparkCubingJob.create(Sets.newHashSet(oneSeg), toBuildLayouts, "ADMIN", null)
    val sparkStep: NSparkCubingStep = job.getSparkCubingStep
    val distMetaUrl: StorageURL = StorageURL.valueOf(sparkStep.getDistMetaUrl)
    Assert.assertEquals("hdfs", distMetaUrl.getScheme)
    Assert.assertTrue(
      distMetaUrl
        .getParameter("path")
        .startsWith(config.getHdfsWorkingDirectory))
    // launch the job
    execMgr.addJob(job)
    if (!Objects.equals(wait(job), ExecutableState.SUCCEED)) {
      val message = job.getTasks.asScala
        .find(executable => Objects.equals(executable.getStatus, ExecutableState.ERROR))
        .map(task => execMgr.getOutputFromHDFSByJobId(job.getId, task.getId, Integer.MAX_VALUE).getVerboseMsg)
        .getOrElse("Unknown Error")
      throw new IllegalStateException(message);
    }

    val buildStore: ResourceStore = ExecutableUtils.getRemoteStore(config, job.getSparkCubingStep)
    val layoutIds: java.util.Set[java.lang.Long] = toBuildLayouts.asScala.map(c => new java.lang.Long(c.getId)).asJava
    UnitOfWork.doInTransactionWithRetry(() => {
      val merger: AfterBuildResourceMerger = new AfterBuildResourceMerger(KylinConfig.getInstanceFromEnv, prj)
      merger.mergeAfterIncrement(df.getUuid, oneSeg.getId, layoutIds, buildStore)
    }, prj)
    checkSnapshotTable(df.getId, oneSeg.getId, oneSeg.getProject)
    oneSeg
  }

  @throws[Exception]
  protected def mergeSegments(cubeName: String,
                              mergeRange: SegmentRange[_ <: Comparable[_]],
                              toBuildLayouts: java.util.Set[LayoutEntity],
                              prj: String): NDataSegment = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, prj)
    val execMgr: ExecutableManager = ExecutableManager.getInstance(config, prj)
    val df: NDataflow = dsMgr.getDataflow(cubeName)
    val mergeSeg = UnitOfWork.doInTransactionWithRetry(() => {
      NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, prj).mergeSegments(df, mergeRange, false)
    }, prj)
    val mergeJob = NSparkMergingJob.merge(mergeSeg,
      Sets.newLinkedHashSet(toBuildLayouts),
      "ADMIN", RandomUtil.randomUUIDStr)
    execMgr.addJob(mergeJob)
    // wait job done
    Assert.assertEquals(ExecutableState.SUCCEED, wait(mergeJob))
    UnitOfWork.doInTransactionWithRetry(() => {
      val merger = new AfterMergeOrRefreshResourceMerger(KylinConfig.getInstanceFromEnv, prj)
      merger.merge(mergeJob.getTask(classOf[NSparkMergingStep]))
    }, prj)
    mergeSeg
  }

  @throws[InterruptedException]
  protected def wait(job: AbstractExecutable): ExecutableState = {
    while (true) {
      Thread.sleep(500)
      val status = job.getStatus
      if (!status.isProgressing) {
        return status
      }
    }
    null
  }

  @throws[Exception]
  def buildOneSegmentForCubePlanner(dfName: String,
                                    prj: String = DEFAULT_PROJECT): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    val df = dsMgr.getDataflow(dfName)
    Assert.assertTrue(config.getHdfsWorkingDirectory.startsWith("file:"))

    // cleanup all segments first
    UnitOfWork.doInTransactionWithRetry(() => {
      val update = new NDataflowUpdate(df.getUuid)
      update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
      NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, prj).updateDataflow(update)
    }, prj)

    // build one segment for cube planner
    val layouts = df.getIndexPlan.getAllLayouts
    val start = SegmentRange.dateToLong("2010-01-01")
    val end = SegmentRange.dateToLong("2023-01-01")
    val segment = buildSegment(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    logInfo(s"build cube planner segment: $segment")

    // validate the first segment for build
    val firstSegment = dsMgr.getDataflow(dfName).getSegments().get(0)
    Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(
      SegmentRange.dateToLong("2010-01-01"),
      SegmentRange.dateToLong("2023-01-01")),
      firstSegment.getSegRange)
  }

  @throws[Exception]
  def buildFourSegementAndMerge(dfName: String,
                                prj: String = DEFAULT_PROJECT): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    var df = dsMgr.getDataflow(dfName)
    Assert.assertTrue(config.getHdfsWorkingDirectory.startsWith("file:"))
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    UnitOfWork.doInTransactionWithRetry(() => {
      NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, DEFAULT_PROJECT).updateDataflow(update)
    }, prj)

    /**
     * Round1. Build 4 segment
     */
    val layouts = df.getIndexPlan.getAllLayouts
    var start = SegmentRange.dateToLong("2010-01-01")
    var end = SegmentRange.dateToLong("2012-06-01")
    var segment = buildCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    val seg1 = checkSourceBytesSize(segment)

    start = SegmentRange.dateToLong("2012-06-01")
    end = SegmentRange.dateToLong("2013-01-01")
    segment = buildCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    val seg2 = checkSourceBytesSize(segment)

    start = SegmentRange.dateToLong("2013-01-01")
    end = SegmentRange.dateToLong("2013-06-01")
    segment = buildCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    val seg3 = checkSourceBytesSize(segment)

    start = SegmentRange.dateToLong("2013-06-01")
    end = SegmentRange.dateToLong("2015-01-01")
    segment = buildCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    val seg4 = checkSourceBytesSize(segment)

    /**
     * Round2. Merge two segments
     */
    df = dsMgr.getDataflow(dfName)

    val firstMergeRange = new SegmentRange.TimePartitionedSegmentRange(
      SegmentRange.dateToLong("2010-01-01"),
      SegmentRange.dateToLong("2013-01-01"))
    mergeSegments(dfName, firstMergeRange, Sets.newLinkedHashSet(layouts), prj)

    val secondMergeRange = new SegmentRange.TimePartitionedSegmentRange(
      SegmentRange.dateToLong("2013-01-01"),
      SegmentRange.dateToLong("2015-01-01"))
    mergeSegments(dfName, secondMergeRange, Sets.newLinkedHashSet(layouts), prj)

    /**
     * validate cube segment info
     */
    val firstSegment = dsMgr.getDataflow(dfName).getSegments().get(0)
    val secondSegment = dsMgr.getDataflow(dfName).getSegments().get(1)
    Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(
      SegmentRange.dateToLong("2010-01-01"),
      SegmentRange.dateToLong("2013-01-01")),
      firstSegment.getSegRange)
    Assert.assertEquals(firstSegment.getSourceBytesSize, seg1 + seg2)
    Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(
      SegmentRange.dateToLong("2013-01-01"),
      SegmentRange.dateToLong("2015-01-01")),
      secondSegment.getSegRange)
    Assert.assertEquals(secondSegment.getSourceBytesSize, seg3 + seg4)
    //    Assert.assertEquals(21, firstSegment.getDictionaries.size)
    //    Assert.assertEquals(21, secondSegment.getDictionaries.size)
    df.getModel.getLookupTables.asScala.foreach(table => Assert.assertTrue(table.getTableDesc.getLastSnapshotPath != null))
  }

  def buildAll(): Unit = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val manager = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    val allModel = manager.listAllDataflows().asScala.map(_.getUuid).toList
    buildCubes(allModel)
  }

  def dumpMetadata(): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val metadataUrlPrefix = config.getMetadataUrlPrefix
    val metadataUrl = metadataUrlPrefix + "/metadata"
    FileUtils.deleteQuietly(new File(metadataUrl))
    val resourceStore: ResourceStore = ResourceStore.getKylinMetaStore(config)
    val outputConfig: KylinConfig = KylinConfig.createKylinConfig(config)
    outputConfig.setMetadataUrl(metadataUrlPrefix)
    MetadataStore.createMetadataStore(outputConfig).dump(resourceStore)
  }

  def checkSourceBytesSize(nDataSegment: NDataSegment): Long = {
    val metadataDir = System.getProperty(KylinConfig.KYLIN_CONF)
    val expect = nDataSegment.getModel.getAllTableRefs.asScala
      .toSeq
      .map(table => new File(metadataDir, "data/" + table.getTableIdentity + ".csv").length())
    val dataSegment = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, nDataSegment.getProject)
      .getDataflow(nDataSegment.getDataflow.getId)
      .getSegment(nDataSegment.getId)
    assert(dataSegment.getSourceBytesSize == expect.sum,
      s"Error for get source bytes size,it should $expect, actual is ${dataSegment.getSourceBytesSize}")
    dataSegment.getSourceBytesSize
  }

  def checkOrder(sparkSession: SparkSession, dfName: String, project: String): Unit = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, project)
    val df: NDataflow = dsMgr.getDataflow(dfName)
    val latestReadySegment: NDataSegment = df.getQueryableSegments.getFirstSegment
    val allCuboidLayouts = df.getIndexPlan.getAllLayouts.asScala
    val base: String = KapConfig.getInstanceFromEnv.getReadParquetStoragePath(df.getProject)
    for (nCuboidLayout <- allCuboidLayouts) {
      val path: String = TableScanPlan.toLayoutPath(df, nCuboidLayout.getId, base, latestReadySegment)
      val paths = new Path(path).getFileSystem(new Configuration())
        .listFiles(new Path(path), false)
      while (paths.hasNext) {
        val path = paths.next()
        val beforeSort = sparkSession.read.parquet(path.getPath.toString)
        val afterSort = beforeSort.sort(beforeSort.schema.names.map(col): _*)
        val str = SparderQueryTest.checkAnswer(beforeSort, afterSort, checkOrder = true)
        if (str != null) {
          // scalastyle:off println
          beforeSort.collect().foreach(println)
          println("==========================")
          afterSort.collect().foreach(println)
          // scalastyle:on println
          dumpMetadata()
          throw new RuntimeException(s"Check order failed : dfName: $dfName, layoutId: ${nCuboidLayout.getId}")
        }
      }
    }
  }
}
