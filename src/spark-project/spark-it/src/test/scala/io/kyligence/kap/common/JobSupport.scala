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

package io.kyligence.kap.common

import java.io.File
import java.util.stream.Collectors
import java.util.{Objects, UUID}
import com.google.common.collect.{Lists, Maps, Sets}
import io.kyligence.kap.engine.spark.job.{NSparkCubingJob, NSparkCubingStep, NSparkMergingJob, NSparkMergingStep}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.persistence.ResourceStore
import org.apache.kylin.common.{KapConfig, KylinConfig, StorageURL}
import org.apache.kylin.job.engine.JobEngineConfig
import org.apache.kylin.job.execution.{AbstractExecutable, ExecutableState, NExecutableManager}
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler
import org.apache.kylin.metadata.model.{SegmentRange, TableDesc}
import org.apache.kylin.metadata.realization.RealizationStatusEnum
import org.apache.kylin.common.persistence.metadata.MetadataStore
import org.apache.kylin.common.util.{RandomUtil, Unsafe}
import org.apache.kylin.engine.spark.ExecutableUtils
import org.apache.kylin.engine.spark.merger.{AfterBuildResourceMerger, AfterMergeOrRefreshResourceMerger}
import org.apache.kylin.engine.spark.utils.{FileNames, HDFSUtils}
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataSegment, NDataflow, NDataflowManager, NDataflowUpdate}
import org.apache.kylin.query.runtime.plan.TableScanPlan
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.common.SparderQueryTest
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

trait JobSupport
  extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging {
  self: Suite =>
  val DEFAULT_PROJECT = "default"
  val schedulerInterval = "1"
  var scheduler: NDefaultScheduler = _
  val systemProp = Maps.newHashMap[String, String]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    Unsafe.setProperty("kylin.job.scheduler.poll-interval-second", schedulerInterval)
    scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT)
    scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv))
    if (!scheduler.hasStarted) {
      throw new RuntimeException("scheduler has not been started")
    }
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
    scheduler.shutdown()
    restoreSparderEnv()
    super.afterAll()

  }

  @throws[Exception]
  def changeCubeStatus(dfName: String,
                       status: RealizationStatusEnum,
                       prj: String = DEFAULT_PROJECT): Unit = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, prj)
    var df: NDataflow = dsMgr.getDataflow(dfName)
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
    dsMgr.updateDataflow(update)
    df = dsMgr.getDataflow(dfName)
    val layouts: java.util.List[LayoutEntity] =
      df.getIndexPlan.getAllLayouts
    val round1: java.util.List[LayoutEntity] = Lists.newArrayList(layouts)
    builCuboid(dfName,
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
        Assert.assertTrue(s"$parent should ony one file, please check " +
          s"org.apache.kylin.engine.spark.merger.AfterBuildResourceMerger.updateSnapshotTableIfNeed ",
          HDFSUtils.listSortedFileFrom(parent).size == 1)
    })
  }

  @throws[Exception]
  protected def builCuboid(cubeName: String,
                           segmentRange: SegmentRange[_ <: Comparable[_]],
                           toBuildLayouts: java.util.Set[LayoutEntity],
                           prj: String): NDataSegment = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, prj)
    val execMgr: NExecutableManager =
      NExecutableManager.getInstance(config, prj)
    val df: NDataflow = dsMgr.getDataflow(cubeName)
    // ready dataflow, segment, cuboid layout
    val oneSeg: NDataSegment = dsMgr.appendSegment(df, segmentRange)
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
    val merger: AfterBuildResourceMerger = new AfterBuildResourceMerger(config, prj)
    val layoutIds: java.util.Set[java.lang.Long] = toBuildLayouts.asScala.map(c => new java.lang.Long(c.getId)).asJava
    merger.mergeAfterIncrement(df.getUuid, oneSeg.getId, layoutIds, buildStore)
    checkSnapshotTable(df.getId, oneSeg.getId, oneSeg.getProject)
    oneSeg
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
  def buildFourSegementAndMerge(dfName: String,
                                prj: String = DEFAULT_PROJECT): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    val execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT)
    var df = dsMgr.getDataflow(dfName)
    Assert.assertTrue(config.getHdfsWorkingDirectory.startsWith("file:"))
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    /**
     * Round1. Build 4 segment
     */
    val layouts = df.getIndexPlan.getAllLayouts
    var start = SegmentRange.dateToLong("2010-01-01")
    var end = SegmentRange.dateToLong("2012-06-01")
    var segment = builCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    val seg1 = checkSourceBytesSize(segment)

    start = SegmentRange.dateToLong("2012-06-01")
    end = SegmentRange.dateToLong("2013-01-01")
    segment = builCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    val seg2 = checkSourceBytesSize(segment)

    start = SegmentRange.dateToLong("2013-01-01")
    end = SegmentRange.dateToLong("2013-06-01")
    segment = builCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    val seg3 = checkSourceBytesSize(segment)

    start = SegmentRange.dateToLong("2013-06-01")
    end = SegmentRange.dateToLong("2015-01-01")
    segment = builCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet(layouts),
      prj)
    val seg4 = checkSourceBytesSize(segment)

    /**
     * Round2. Merge two segments
     */
    df = dsMgr.getDataflow(dfName)

    val firstMergeSeg = dsMgr.mergeSegments(
      df,
      new SegmentRange.TimePartitionedSegmentRange(
        SegmentRange.dateToLong("2010-01-01"),
        SegmentRange.dateToLong("2013-01-01")),
      false)
    val firstMergeJob = NSparkMergingJob.merge(firstMergeSeg,
      Sets.newLinkedHashSet(layouts),
      "ADMIN", RandomUtil.randomUUIDStr)
    execMgr.addJob(firstMergeJob)
    // wait job done
    Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob))
    val merger = new AfterMergeOrRefreshResourceMerger(config, prj)
    merger.merge(firstMergeJob.getTask(classOf[NSparkMergingStep]))

    df = dsMgr.getDataflow(dfName)
    val secondMergeSeg = dsMgr.mergeSegments(
      df,
      new SegmentRange.TimePartitionedSegmentRange(
        SegmentRange.dateToLong("2013-01-01"),
        SegmentRange.dateToLong("2015-01-01")),
      false)
    val secondMergeJob = NSparkMergingJob.merge(secondMergeSeg,
      Sets.newLinkedHashSet(layouts),
      "ADMIN", RandomUtil.randomUUIDStr)
    execMgr.addJob(secondMergeJob)
    Assert.assertEquals(ExecutableState.SUCCEED, wait(secondMergeJob))
    merger.merge(secondMergeJob.getTask(classOf[NSparkMergingStep]))

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


  @throws[Exception]
  def buildTwoSegementAndMerge(dfName: String,
                               prj: String = DEFAULT_PROJECT): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT)
    val execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT)
    var df = dsMgr.getDataflow(dfName)
    Assert.assertTrue(config.getHdfsWorkingDirectory.startsWith("file:"))
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    /**
     * Round1. Build 4 segment
     */
    val layouts = df.getIndexPlan.getAllLayouts
    var start = SegmentRange.dateToLong("2010-01-01")
    var end = SegmentRange.dateToLong("2013-01-01")
    builCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet[LayoutEntity](layouts),
      prj)
    start = SegmentRange.dateToLong("2013-01-01")
    end = SegmentRange.dateToLong("2015-01-01")
    builCuboid(dfName,
      new SegmentRange.TimePartitionedSegmentRange(start, end),
      Sets.newLinkedHashSet[LayoutEntity](layouts),
      prj)

    /**
     * Round2. Merge two segments
     */
    df = dsMgr.getDataflow(dfName)
    val firstMergeSeg = dsMgr.mergeSegments(
      df,
      new SegmentRange.TimePartitionedSegmentRange(
        SegmentRange.dateToLong("2010-01-01"),
        SegmentRange.dateToLong("2015-01-01")),
      false)
    val firstMergeJob = NSparkMergingJob.merge(firstMergeSeg,
      Sets.newLinkedHashSet(layouts),
      "ADMIN", RandomUtil.randomUUIDStr)
    execMgr.addJob(firstMergeJob)
    // wait job done
    Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob))
    val merger = new AfterMergeOrRefreshResourceMerger(config, prj)
    merger.merge(firstMergeJob.getTask(classOf[NSparkMergingStep]))

    /**
     * validate cube segment info
     */
    val firstSegment = dsMgr.getDataflow(dfName).getSegments().get(0)
    Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(
      SegmentRange.dateToLong("2010-01-01"),
      SegmentRange.dateToLong("2015-01-01")),
      firstSegment.getSegRange)
    //    Assert.assertEquals(21, firstSegment.getDictionaries.size)
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
        val str = SparderQueryTest.checkAnswer(beforeSort, afterSort, true)
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
