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

package org.apache.kylin.engine.spark.job

import com.google.common.collect.{Lists, Queues}
import org.apache.kylin.engine.spark.job.SegmentExec.{LayoutResult, ResultType, SourceStats}
import org.apache.kylin.engine.spark.job.stage.merge.MergeStage
import org.apache.kylin.engine.spark.scheduler.JobRuntime
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.NDataModel
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.internal.Logging
import org.apache.spark.sql.datasource.storage.{StorageListener, StorageStoreFactory, WriteTaskStats}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import java.util
import java.util.Objects
import java.util.concurrent.ForkJoinPool
import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport

trait SegmentExec extends Logging {

  protected val jobId: String
  protected val project: String
  protected val segmentId: String
  protected val dataflowId: String

  protected val config: KylinConfig
  protected val sparkSession: SparkSession

  protected val dataModel: NDataModel
  protected val storageType: Int

  protected def runtime: JobRuntime

  @volatile protected var backgroundFailure: Option[Throwable] = None

  // Layout result pipe.
  protected final lazy val pipe = Queues.newLinkedBlockingQueue[ResultType]()

  // Await or fail fast.
  private lazy val noneOrFailure = Queues.newLinkedBlockingQueue[Option[Throwable]]()

  protected def awaitOrFailFast(countDown: Int): Unit = {
    // Await layer or fail fast.
    var i = countDown
    while (i > 0) {
      val failure = noneOrFailure.take()
      if (failure.nonEmpty) {
        val t = failure.get
        logError(s"Segment $segmentId fail fast.", t)
        drain()
        throw t
      }
      i -= 1
    }
  }

  protected final def asyncExecute(f: => Unit): Unit = {
    runtime.submit(() => try {
      setConfig4CurrentThread()
      f
      noneOrFailure.offer(None)
    } catch {
      case t: Throwable => noneOrFailure.offer(Some(t))
    })
  }

  protected final def setConfig4CurrentThread(): Unit = {
    if (KylinConfig.isKylinConfigThreadLocal) {
      // Already set, do nothing.
      return
    }
    KylinConfig.setAndUnsetThreadLocalConfig(config)
  }

  protected def drain(): Unit = synchronized {
    var entry = pipe.poll()
    if (Objects.isNull(entry)) {
      return
    }
    val results = Lists.newArrayList(entry.asInstanceOf[LayoutResult])
    entry = pipe.poll()
    while (Objects.nonNull(entry)) {
      results.add(entry.asInstanceOf[LayoutResult])
      entry = pipe.poll()
    }
    logInfo(s"Segment $segmentId drained layouts: " + //
      s"${results.asScala.map(_.layoutId).mkString("[", ",", "]")}")

    class DFUpdate extends UnitOfWork.Callback[Int] {
      override def process(): Int = {

        // Merge into the newest data segment.
        val manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, project)
        val copiedDataflow = manager.getDataflow(dataflowId).copy()

        val dataLayouts = results.asScala.map { lr =>
          val layoutId = lr.layoutId
          val taskStats = lr.stats
          val sourceStats = lr.sourceStats
          val dataLayout = NDataLayout.newDataLayout(copiedDataflow, segmentId, layoutId)
          // Job id should be set.
          dataLayout.setBuildJobId(jobId)
          if (taskStats.numRows == -1) {
            KylinBuildEnv.get().buildJobInfos.recordAbnormalLayouts(layoutId, "Total row count -1.")
            logWarning(s"Segment $segmentId layout $layoutId total row count -1.")
          }
          dataLayout.setSourceRows(sourceStats.rows)

          dataLayout.setRows(taskStats.numRows)
          dataLayout.setPartitionNum(taskStats.numBucket)
          dataLayout.setPartitionValues(taskStats.partitionValues)
          dataLayout.setFileCount(taskStats.numFiles)
          dataLayout.setByteSize(taskStats.numBytes)
          dataLayout
        }
        updateDataLayouts(manager, dataLayouts)
      }
    }
    UnitOfWork.doInTransactionWithRetry(new DFUpdate, project)
    logDebug(s"Segment $segmentId update metadata ${results.asScala.map(_.layoutId).mkString("[", ",", "]")}.")
  }

  protected final def updateDataLayouts(manager: NDataflowManager, dataLayouts: Seq[NDataLayout]): Int = {
    val updates = new NDataflowUpdate(dataflowId)
    updates.setToAddOrUpdateLayouts(dataLayouts: _*)
    manager.updateDataflow(updates)
    0
  }

  protected def scheduleCheckpoint(): Unit = {
    // Collect and merge layout built results, then checkpoint.
    runtime.scheduleCheckpoint(() => try {
      setConfig4CurrentThread()
      drain()
    } catch {
      case t: Throwable => logError(s"Segment $segmentId checkpoint failed.", t); backgroundFailure = Some(t); throw t
    })
  }

  protected final def wrapLayoutDS(layout: LayoutEntity, parentDS: Dataset[Row]): Dataset[Row] = {
    if (IndexEntity.isTableIndex(layout.getId)) {
      require(layout.getIndex.getMeasures.isEmpty)
      wrapTblLayoutDS(layout, parentDS)
    } else {
      wrapAggLayoutDS(layout, parentDS)
    }
  }

  private def wrapTblLayoutDS(layout: LayoutEntity, parentDS: Dataset[Row]): Dataset[Row] = {
    require(layout.getIndex.getMeasures.isEmpty)
    val dimensions = wrapDimensions(layout)
    val columns = NSparkCubingUtil.getColumns(dimensions)
    parentDS.select(columns: _*).sortWithinPartitions(columns: _*)
  }

  protected def columnIdFunc(colRef: TblColRef): String

  private def wrapAggLayoutDS(layout: LayoutEntity, parentDS: Dataset[Row]): Dataset[Row] = {
    val dimensions = wrapDimensions(layout)
    val measures = layout.getOrderedMeasures.keySet()
    val sortColumns = NSparkCubingUtil.getColumns(dimensions)
    val selectColumns = NSparkCubingUtil.getColumns(NSparkCubingUtil.combineIndices(dimensions, measures))
    val aggregated = CuboidAggregator.aggregate(parentDS, //
      dimensions, layout.getIndex.getEffectiveMeasures, columnIdFunc)
    aggregated.select(selectColumns: _*).sortWithinPartitions(sortColumns: _*)
  }

  protected final def newDataLayout(segment: NDataSegment, //
                                    layout: LayoutEntity, //
                                    layoutDS: Dataset[Row], //
                                    readableDesc: String,
                                    storageListener: Option[StorageListener]): Unit = {
    val storagePath = NSparkCubingUtil.getStoragePath(segment, layout.getId)
    val taskStats = saveWithStatistics(layout, layoutDS, storagePath, readableDesc, storageListener)
    val sourceStats = newSourceStats(layout, taskStats)
    pipe.offer(LayoutResult(layout.getId, taskStats, sourceStats))
  }

  protected def newSourceStats(layout: LayoutEntity, taskStats: WriteTaskStats): SourceStats = {
    logInfo(s"Segment $segmentId layout source rows ${layout.getId} ${taskStats.sourceRows}")
    SourceStats(rows = taskStats.sourceRows)
  }

  protected def wrapDimensions(layout: LayoutEntity): util.Set[Integer] = {
    val dimensions = layout.getOrderedDimensions.keySet()
    logInfo(s"Segment $segmentId layout dimensions ${layout.getId} ${dimensions.asScala.mkString("[", ",", "]")}")
    dimensions
  }

  protected val sparkSchedulerPool: String

  protected final def saveWithStatistics(layout: LayoutEntity, layoutDS: Dataset[Row], //
                                         storagePath: String, readableDesc: String,
                                         storageListener: Option[StorageListener]): WriteTaskStats = {
    logInfo(readableDesc)
    sparkSession.sparkContext.setJobDescription(readableDesc)
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", sparkSchedulerPool)
    val store = StorageStoreFactory.create(storageType)
    storageListener match {
      case Some(x) => store.setStorageListener(x)
      case None =>
    }

    val stats = store.save(layout, new Path(storagePath), KapConfig.wrap(config), layoutDS)
    sparkSession.sparkContext.setJobDescription(null)
    stats
  }

  private def intersectDimensions(dimensions: util.Set[Integer], ds: Dataset[Row]): util.Set[Integer] = {
    if (this.isInstanceOf[MergeStage]) {
      val fieldNames = ds.schema.fieldNames.toSet
      val dimensionsStrings = dimensions.asScala.map(dim => String.valueOf(dim)).toSet
      val intersection = fieldNames.intersect(dimensionsStrings).map(dim => Integer.valueOf(dim))
      return intersection.asJava
    }
    dimensions
  }

  protected def calDimRange(segment: NDataSegment, ds: Dataset[Row]): java.util.HashMap[String, DimensionRangeInfo] = {
    val dimensions = segment.getDataflow.getIndexPlan.getEffectiveDimCols.keySet()
    val dimRangeInfo = new java.util.HashMap[String, DimensionRangeInfo]
    // Not support multi partition for now
    if (Objects.isNull(segment.getModel.getMultiPartitionDesc)
      && config.isDimensionRangeFilterEnabled
      && !dimensions.isEmpty) {
      val start = System.currentTimeMillis()
      import org.apache.spark.sql.functions._

      val intersectionDimensions = intersectDimensions(dimensions, ds)
      val columns = NSparkCubingUtil.getColumns(intersectionDimensions)
      val dimDS = ds.select(columns: _*)

      // Calculate max and min of all dimensions
      val minCols: Array[Column] = dimDS.columns.map(min)
      val maxCols: Array[Column] = dimDS.columns.map(max)
      val cols = Array.concat(minCols, maxCols)
      val row = dimDS.agg(cols.head, cols.tail: _*).head.toSeq.splitAt(columns.length)
      (intersectionDimensions.asScala.toSeq, row._1, row._2)
        .zipped.map {
        case (_, null, null) =>
        case (column, min, max) => dimRangeInfo.put(column.toString, new DimensionRangeInfo(min.toString, max.toString))
      }
      val timeCost = System.currentTimeMillis() - start
      logInfo(s"Segment $segmentId calculate dimension range cost $timeCost ms")
    }
    dimRangeInfo
  }

  protected def cleanup(): Unit = {
    drain()
  }

  protected def cleanupLayoutTempData(segment: NDataSegment, layouts: Seq[LayoutEntity]): Unit = {
    logInfo(s"Segment $segmentId cleanup layout temp data.")
    val prefixes = layouts.map(_.getId).map(id => s"${id}_temp")
    val segmentPath = new Path(NSparkCubingUtil.getStoragePath(segment))
    val fileSystem = segmentPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(segmentPath)) {
      return
    }
    val cleanups = fileSystem.listStatus(segmentPath, new PathFilter {
      override def accept(destPath: Path): Boolean = {
        val name = destPath.getName
        prefixes.exists(prefix => name.startsWith(prefix))
      }
    }).map(_.getPath)

    if (cleanups.isEmpty) {
      return
    }
    val processors = Runtime.getRuntime.availableProcessors
    val parallel = cleanups.par
    val forkJoinPool = new ForkJoinPool(Math.max(processors, cleanups.length / 2))
    try {
      parallel.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
      parallel.foreach { p =>
        fileSystem.delete(p, true)
      }
    } finally {
      forkJoinPool.shutdownNow()
    }
    logInfo(s"Segment $segmentId cleanup layout temp data: ${cleanups.map(_.getName).mkString("[", ",", "]")}")
  }

}

object SegmentExec {

  trait ResultType

  case class SourceStats(rows: Long)

  case class LayoutResult(layoutId: java.lang.Long, stats: WriteTaskStats, sourceStats: SourceStats) extends ResultType

}
