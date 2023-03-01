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

package org.apache.kylin.engine.spark.job.stage.merge

import org.apache.hadoop.fs.Path
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.common.persistence.transaction.UnitOfWork.Callback
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.SegmentExec.SourceStats
import org.apache.kylin.engine.spark.job.stage.StageExec
import org.apache.kylin.engine.spark.job.{SegmentExec, SegmentJob}
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.TblColRef
import org.apache.kylin.metadata.sourceusage.SourceUsageManager
import org.apache.spark.sql.datasource.storage.{StorageStoreUtils, WriteTaskStats}
import org.apache.spark.sql.{Dataset, Row, SaveMode}

import java.io.IOException
import java.lang
import java.util.Objects
import scala.collection.JavaConverters
import scala.collection.JavaConverters._

abstract class MergeStage(private val jobContext: SegmentJob,
                          private val dataSegment: NDataSegment)
  extends SegmentExec with StageExec {
  override def getJobContext: SparkApplication = jobContext

  override def getDataSegment: NDataSegment = dataSegment

  override def getSegmentId: String = dataSegment.getId

  protected final val jobId = jobContext.getJobId
  protected final val config = jobContext.getConfig
  protected final val dataflowId = jobContext.getDataflowId
  protected final val sparkSession = jobContext.getSparkSession
  protected final val resourceContext = jobContext.getBuildContext
  protected final val runtime = jobContext.getRuntime

  protected final val project = dataSegment.getProject
  protected final val segmentId = dataSegment.getId

  protected final val dataModel = dataSegment.getModel
  protected final val storageType = dataModel.getStorageType


  protected final val unmerged = {
    val segments = jobContext.getUnmergedSegments(dataSegment).asScala
    logInfo(s"Unmerged SEGMENT [${segments.map(_.getId).mkString(",")}]")
    segments
  }

  protected case class LayoutMergeTask(grouped: Seq[NDataLayout]) extends Task {
    override def getTaskDesc: String = {
      s"${grouped.head.getLayoutId}"
    }
  }

  protected def mergeIndices(): Unit = {
    // Cleanup previous potentially left temp layout data.
    cleanupLayoutTempData(dataSegment, jobContext.getReadOnlyLayouts.asScala.toSeq)

    val tasks = unmerged.flatMap(segment => segment.getSegDetails.getEffectiveLayouts.asScala) //
      .groupBy(_.getLayoutId).values.map(LayoutMergeTask)
    slowStartExec(tasks.iterator, mergeLayout)
  }

  override protected def recordTaskInfo(t: Task): Unit = {
    logInfo(s"Segment $segmentId submit task: ${t.getTaskDesc}")
  }

  private def mergeLayout(task: LayoutMergeTask): Unit = {
    val head = task.grouped.head
    val layout = head.getLayout
    val layoutId = layout.getId
    val unitedDS: Dataset[Row] = newUnitedDS(layoutId)
    if (Objects.isNull(unitedDS)) {
      return
    }
    mergeDataLayout(layout, unitedDS)
  }

  private def newUnitedDS(layoutId: lang.Long): Dataset[Row] = {
    var unitedDS: Dataset[Row] = null
    unmerged.foreach { segment =>
      val dataLayout = segment.getLayout(layoutId)
      if (Objects.isNull(dataLayout)) {
        logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Layout not found " +
          s"layout $layoutId segment ${segment.getId}")
      } else {
        val layout = dataLayout.getLayout
        val layoutDS = StorageStoreUtils.toDF(segment, layout, sparkSession)
        unitedDS = if (Objects.isNull(unitedDS)) {
          layoutDS
        } else {
          unitedDS.union(layoutDS)
        }
      }
    }
    unitedDS
  }

  private def mergeDataLayout(layout: LayoutEntity, unitedDS: Dataset[Row]): Unit = {
    val readableDesc = s"Merge layout ${layout.getId}"
    val layoutDS = wrapLayoutDS(layout, unitedDS)
    newDataLayout(dataSegment, layout, layoutDS, readableDesc, None)
  }


  override protected def newSourceStats(layout: LayoutEntity, //
                                        origin: WriteTaskStats): SourceStats = {
    val sourceRows = unmerged.map(segment => segment.getLayout(layout.getId)) //
      .filterNot(Objects.isNull) //
      .map(_.getSourceRows).sum
    logInfo(s"Layout ${layout.getId} source rows $sourceRows")
    SourceStats(rows = sourceRows)
  }

  protected val sparkSchedulerPool: String = "merge"

  override protected def columnIdFunc(colRef: TblColRef): String = //
    if (config.isUTEnv) {
      val tableDesc = new SegmentFlatTableDesc(config, dataSegment, null)
      tableDesc.getColumnIdAsString(colRef)
    } else {
      s"-1"
    }

  protected def mergeFlatTable(): Unit = {
    if (!config.isPersistFlatTableEnabled) {
      logInfo(s"Flat table persisting is not enabled.")
      onStageSkipped()
      return
    }
    // Check flat table paths
    val unmergedFTPaths = getUnmergedFTPaths
    if (unmergedFTPaths.isEmpty) {
      onStageSkipped()
      return
    }

    var tableDS = sparkSession.read.parquet(unmergedFTPaths.head.toString)
    val schema = tableDS.schema.fieldNames.mkString(",")
    logInfo(s"FLAT-TABLE schema $schema")
    val schemaMatched = unmergedFTPaths.drop(1).forall { fp =>
      val otherDS = sparkSession.read.parquet(fp.toString)
      val other = otherDS.schema.fieldNames.mkString(",")
      logInfo(s"FLAT-TABLE schema $other")
      schema.equals(other)
    }
    if (!schemaMatched) {
      logWarning("Skip FLAT-TABLE schema not matched.")
      onStageSkipped()
      return
    }

    unmergedFTPaths.drop(1).foreach { fp =>
      val other = sparkSession.read.parquet(fp.toString)
      tableDS = tableDS.union(other)
    }
    // Persist
    val newPath = config.getFlatTableDir(project, dataflowId, segmentId)
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "merge")
    sparkSession.sparkContext.setJobDescription("Persist flat table.")
    tableDS.write.mode(SaveMode.Overwrite).parquet(newPath.toString)

    logInfo(s"Persist merged FLAT-TABLE $newPath with schema $schema")

    val dataflowManager = NDataflowManager.getInstance(config, project)
    val copiedDataflow = dataflowManager.getDataflow(dataflowId).copy()
    val copiedSegment = copiedDataflow.getSegment(segmentId)
    copiedSegment.setFlatTableReady(true)
    val update = new NDataflowUpdate(dataflowId)
    update.setToUpdateSegs(copiedSegment)
    dataflowManager.updateDataflow(update)
  }

  protected def getUnmergedFTPaths: Seq[Path] = { // check flat table ready
    val notReadies = unmerged.filterNot(_.isFlatTableReady).map(_.getId)
    if (notReadies.nonEmpty) {
      logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Merging FLAT-TABLE, " +
        s"but found that some flat table were not ready like [${notReadies.mkString(",")}]")
      return Seq.empty[Path]
    }
    // check flat table exists
    val fs = if (config.isBuildFilesSeparationEnabled) {
      HadoopUtil.getWritingClusterFileSystem
    } else {
      HadoopUtil.getWorkingFileSystem
    }
    val notExists = unmerged.filterNot { segment =>
      def exists(segment: NDataSegment) = try {
        val pathFT = config.getFlatTableDir(project, dataflowId, segment.getId)
        fs.exists(pathFT)
      } catch {
        case ioe: IOException =>
          logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Checking FLAT-TABLE exists of segment ${segment.getId}", ioe)
          false
      }

      exists(segment)
    }.map(_.getId)
    if (notExists.nonEmpty) {
      logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Merging FLAT-TABLE, "
        + s"but found that some flat table were not exists like [${notExists.mkString(",")}]")
      return Seq.empty[Path]
    }
    unmerged.map(segment => config.getFlatTableDir(project, dataflowId, segment.getId))
  }

  private def mergeDimRange(): java.util.Map[String, DimensionRangeInfo] = {
    val emptyDimRangeSeg = unmerged.filter(seg => seg.getDimensionRangeInfoMap.isEmpty)
    val dataflow = NDataflowManager.getInstance(config, project).getDataflow(dataflowId)
    val mergedSegment = dataflow.getSegment(segmentId)
    if (mergedSegment.isFlatTableReady) {
      val flatTablePath = config.getFlatTableDir(project, dataflowId, segmentId)
      val mergedDS = sparkSession.read.parquet(flatTablePath.toString)
      calDimRange(mergedSegment, mergedDS)
    } else if (emptyDimRangeSeg.nonEmpty) {
      new java.util.HashMap[String, DimensionRangeInfo]
    } else {
      val dimCols = dataflow.getIndexPlan.getEffectiveDimCols
      val mergedDimRange = unmerged.map(seg => JavaConverters.mapAsScalaMap(seg.getDimensionRangeInfoMap).toSeq)
        .reduce(_ ++ _).groupBy(_._1).mapValues(_.map(_._2).seq)
        .filter(dim => dimCols.containsKey(Integer.parseInt(dim._1)))
        .map(dim => (dim._1, dim._2.reduce(_.merge(_, dimCols.get(Integer.parseInt(dim._1)).getType))))
      JavaConverters.mapAsJavaMap(mergedDimRange)
    }
  }

  protected def mergeColumnBytes(): Unit = {
    UnitOfWork.doInTransactionWithRetry(new Callback[Unit] {
      override def process(): Unit = {
        val usageManager = SourceUsageManager.getInstance(config)
        val totalCount = unmerged.map(_.getSourceCount).sum
        val evaluated = unmerged.flatMap { segment => //
          val existed = if (segment.getColumnSourceBytes.isEmpty) {
            usageManager.calcAvgColumnSourceBytes(segment)
          } else {
            segment.getColumnSourceBytes
          }
          existed.asScala
        }.groupBy(_._1) //
          .mapValues(_.map(_._2).reduce(_ + _)) //
          .asJava
        val dataflowManager = NDataflowManager.getInstance(config, project)
        val copiedDataflow = dataflowManager.getDataflow(dataflowId).copy()
        val copiedSegment = copiedDataflow.getSegment(segmentId)
        val dataflowUpdate = new NDataflowUpdate(dataflowId)
        copiedSegment.setSourceCount(totalCount)
        copiedSegment.setDimensionRangeInfoMap(mergeDimRange())
        // By design, no fencing.
        copiedSegment.getColumnSourceBytes.putAll(evaluated)
        dataflowUpdate.setToUpdateSegs(copiedSegment)
        logInfo(s"Merge COLUMN-BYTES segment $segmentId")
        // The afterward step would dump the meta to hdfs-store.
        // We should only update the latest meta in mem-store.
        // Make sure the copied dataflow here is the latest.
        dataflowManager.updateDataflow(dataflowUpdate)
      }
    }, project)
  }
}
