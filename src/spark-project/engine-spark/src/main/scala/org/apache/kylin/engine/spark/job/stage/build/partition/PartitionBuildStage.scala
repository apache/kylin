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

package org.apache.kylin.engine.spark.job.stage.build.partition

import com.google.common.collect.{Lists, Queues, Sets}
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.common.persistence.transaction.UnitOfWork.Callback
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.job.stage.build.BuildStage
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase.Statistics
import org.apache.kylin.engine.spark.job.{KylinBuildEnv, PartitionExec, SanityChecker, SegmentJob}
import org.apache.kylin.engine.spark.model.PartitionFlatTableDesc
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.TreeNode
import org.apache.kylin.metadata.cube.cuboid.PartitionSpanningTree
import org.apache.kylin.metadata.cube.cuboid.PartitionSpanningTree.PartitionTreeNode
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.{Dataset, Row}

import java.util.Objects
import scala.collection.JavaConverters._
import scala.collection.mutable

abstract class PartitionBuildStage(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends BuildStage(jobContext, dataSegment, buildParam) with PartitionExec {
  protected final val newBuckets = //
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq

  private lazy val spanningTree: PartitionSpanningTree = buildParam.getPartitionSpanningTree

  private lazy val flatTableDesc: PartitionFlatTableDesc = buildParam.getTableDesc
  private lazy val flatTable: PartitionFlatTableAndDictBase = buildParam.getPartitionFlatTable

  private lazy val sanityResultQueue = Queues.newLinkedBlockingQueue[PartitionSanityResult]()

  // Thread unsafe, read only. Partition flat table.
  private lazy val cachedPartitionDS = buildParam.getCachedPartitionDS

  // Thread unsafe, read only. Partition flat table statistics.
  private lazy val cachedPartitionStats = buildParam.getCachedPartitionStats

  // Thread unsafe
  // [layout, [partition, dataset]]
  private lazy val cachedLayoutDS = buildParam.getCachedLayoutPartitionDS

  // Thread unsafe
  // [layout, [partition, sanity]]
  private lazy val cachedLayoutSanity = buildParam.getCachedLayoutPartitionSanity

  override protected def columnIdFunc(colRef: TblColRef): String = flatTableDesc.getColumnIdAsString(colRef)


  override protected def reportTaskProgress(): Unit = {
    val layoutCount = KylinBuildEnv.get().buildJobInfos.getSeg2cuboidsNumPerLayer.get(segmentId).asScala.sum
    onBuildLayoutSuccess(layoutCount / partitions.size())
  }

  override protected def buildLayouts(): Unit = {

    val taskIter = new BuildTaskIterator[PartitionBuildTask] {

      override def canSpan: Boolean = spanningTree.canSpan

      override def spanNodeSeq(segment: NDataSegment): Seq[TreeNode] = {
        spanningTree.span(segment).asScala
      }

      override def genTask(segment: NDataSegment, node: TreeNode): Seq[PartitionBuildTask] = {
        getPartitionTasks(segment, node.asInstanceOf[PartitionTreeNode])
      }
    }

    slowStartExec(taskIter, buildPartition)
  }

  protected def buildPartitionStatistics(partition: Long //
                                         , partitionFlatTableDS: Dataset[Row]): Statistics = {
    // Maybe exist metadata operations.
    setConfig4CurrentThread()
    flatTable.gatherPartitionStatistics(partition, partitionFlatTableDS)
  }

  override protected def buildSanityCache(): Unit = {
    if (!config.isSanityCheckEnabled) {
      return
    }
    // Collect statistics for root nodes.
    val rootNodes = spanningTree.getRootNodes.asScala.map(_.asInstanceOf[PartitionTreeNode])
    if (rootNodes.isEmpty) {
      return
    }

    logInfo(s"Segment $segmentId build sanity cache.")

    val taskIter = rootNodes.map { node =>
      val layout = node.getLayout
      val partition = node.getPartition
      val partitionDS = getCachedPartitionDS(dataSegment, layout, partition)
      new PartitionSanityTask(layout, partition, partitionDS)
    }.iterator

    slowStartExec(taskIter, (sanityTask: PartitionSanityTask) => {
      val layout = sanityTask.layout
      val partition = sanityTask.partition
      val partitionDS = sanityTask.partitionDS
      val sanityCount = SanityChecker.getCount(partitionDS, layout)
      sanityResultQueue.offer(new PartitionSanityResult(layout.getId, partition, sanityCount))
    })

    val collected = polledResultSeq(sanityResultQueue)
    val sanityMap = mutable.HashMap[Long, mutable.HashMap[Long, Long]]()
    collected.foreach { r =>
      val layout = r.layout
      val partition = r.partition
      val sanityCount = r.sanityCount
      sanityMap.getOrElseUpdate(layout, mutable.HashMap[Long, Long]()).put(partition, sanityCount)
    }

    assert(collected.size == rootNodes.size, //
      s"Collect sanity for root nodes went wrong: ${collected.size} == ${rootNodes.size}")
    buildParam.setCachedLayoutPartitionSanity(Some(sanityMap))
    logInfo(s"Segment $segmentId finished build sanity cache $sanityMap.")
  }

  private def getPartitionTasks(segment: NDataSegment, node: PartitionTreeNode): Seq[PartitionBuildTask] = {
    val layouts = node.getLayouts.asScala // skip layouts
      .filterNot(layout => needSkipPartition(layout.getId, node.getPartition, segment))
    if (layouts.isEmpty) {
      return Seq.empty
    }
    val sanityCount = getCachedPartitionSanity(node)
    if (node.parentIsNull) {
      // Build from flat table
      layouts.map { layout =>
        PartitionBuildTask(layout, node.getPartition, None, cachedPartitionDS(node.getPartition), sanityCount, segment)
      }
    } else {
      // Build from data layout
      val parentLayout = node.getParent.getLayout
      val parentDS = getCachedPartitionDS(segment, parentLayout, node.getPartition)

      layouts.map { layout =>
        PartitionBuildTask(layout, node.getPartition, Some(parentLayout), parentDS, sanityCount, segment)
      }
    }
  }

  private def getCachedPartitionSanity(node: PartitionTreeNode): Long = {
    // Not enabled.
    if (!config.isSanityCheckEnabled) {
      return SanityChecker.SKIP_FLAG
    }

    // From flat table.
    if (Objects.isNull(node.getRootNode)) {
      assert(cachedPartitionStats.contains(node.getPartition), //
        s"Partition flat tale statistics should have been cached: ${node.getPartition}")
      return cachedPartitionStats(node.getPartition).totalCount
    }

    cachedLayoutSanity.map { cached =>
      val rootNode = node.getRootNode.asInstanceOf[PartitionTreeNode]
      val layout = rootNode.getLayout.getId
      val partition = rootNode.getPartition
      assert(cached.contains(layout), //
        s"Root node's layout sanity should have been cached: $layout")
      val cachedPartition = cached(layout)
      assert(cachedPartition.contains(partition), //
        s"Root node's layout partition sanity should have been cached: $layout $partition")
      cachedPartition(partition)
    }.getOrElse(SanityChecker.SKIP_FLAG)
  }

  private def getCachedPartitionDS(segment: NDataSegment, //
                                   layout: LayoutEntity, //
                                   partition: Long): Dataset[Row] = synchronized {
    cachedLayoutDS.getOrElseUpdate(layout.getId, // or update
      mutable.HashMap[Long, Dataset[Row]]()).getOrElseUpdate(partition, // or update
      StorageStoreUtils.toDF(segment, layout, partition, sparkSession))
  }

  sealed case class PartitionBuildTask(layout: LayoutEntity //
                                       , partition: Long //
                                       , parentLayout: Option[LayoutEntity] //
                                       , parentDS: Dataset[Row] //
                                       , sanityCount: Long //
                                       , segment: NDataSegment = dataSegment) extends Task {
    override def getTaskDesc: String = {
      s"layout ${layout.getId} partition $partition"
    }
  }


  private def buildPartition(task: PartitionBuildTask): Unit = {
    val layoutDS = wrapLayoutDS(task.layout, task.parentDS)
    val parentDesc = if (task.parentLayout.isEmpty) "flat table" else task.parentLayout.get.getId
    val readableDesc = s"Segment $segmentId build layout partition ${task.layout.getId},${task.partition} from $parentDesc"
    // set layout mess in build job infos
    val layout: NDataLayout = if (task.parentLayout.isEmpty) null
    else task.segment.getLayout(task.parentLayout.get.getId)
    val layoutIdsFromFlatTable = KylinBuildEnv.get().buildJobInfos.getParent2Children.getOrDefault(layout, Sets.newHashSet())
    layoutIdsFromFlatTable.add(task.layout.getId)
    KylinBuildEnv.get().buildJobInfos.recordParent2Children(layout, layoutIdsFromFlatTable)
    newLayoutPartition(task.segment, task.layout, task.partition, layoutDS, readableDesc, Some(new SanityChecker(task.sanityCount)))
  }

  private def needSkipPartition(layout: Long, partition: Long, segment: NDataSegment = dataSegment): Boolean = {
    // Check layout data.
    val dataLayout = segment.getLayout(layout)
    if (Objects.isNull(dataLayout)) {
      return false
    }

    // Check partition data.
    val dataPartition = dataLayout.getDataPartition(partition)
    if (Objects.isNull(dataPartition)) {
      return false
    }

    // Check job id.
    if (jobId.equals(dataPartition.getBuildJobId)) {
      logInfo(s"Segment $segmentId skip build layout partition $layout $partition")
      return true
    }
    false
  }

  override protected def tryRefreshColumnBytes(): Unit = {
    if (cachedPartitionStats.isEmpty) {
      logInfo(s"Segment $segmentId skip refresh column bytes.")
      return
    }
    logInfo(s"Segment $segmentId refresh column bytes.")
    val partitionStats = cachedPartitionStats
    UnitOfWork.doInTransactionWithRetry(new Callback[Unit] {
      override def process(): Unit = {
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val copiedDataflow = dataflowManager.getDataflow(dataflowId).copy()
        val copiedSegment = copiedDataflow.getSegment(segmentId)
        val dataflowUpdate = new NDataflowUpdate(dataflowId)
        val newAdds = Lists.newArrayList[SegmentPartition]()
        partitionStats.foreach { case (partitionId, stats) => //
          val segmentPartition = newSegmentPartition(copiedSegment, partitionId, newAdds)
          segmentPartition.setSourceCount(stats.totalCount)
          // By design, no fencing.
          val columnBytes = segmentPartition.getColumnSourceBytes
          stats.columnBytes.foreach(kv => columnBytes.put(kv._1, kv._2))
        }
        copiedSegment.getMultiPartitions.addAll(newAdds)
        mergeSegmentStatistics(copiedSegment)
        dataflowUpdate.setToUpdateSegs(copiedSegment)
        // The afterward step would dump the meta to hdfs-store.
        // We should only update the latest meta in mem-store.
        // Make sure the copied dataflow here is the latest.
        dataflowManager.updateDataflow(dataflowUpdate)
      }
    }, project)
  }

  override protected def cleanup(): Unit = {

    super.cleanup()

    // Cleanup extra files.
    val fs = HadoopUtil.getWorkingFileSystem
    // Fact table view.
    val ftvPath = flatTableDesc.getFactTableViewPath
    if (fs.exists(ftvPath)) {
      fs.delete(ftvPath, true)
    }

    // Flat table.
    val ftPath = flatTableDesc.getFlatTablePath
    if (fs.exists(ftPath)) {
      fs.delete(ftPath, true)
    }
  }

  sealed class PartitionSanityTask(val layout: LayoutEntity,
                                   val partition: Long,
                                   val partitionDS: Dataset[Row]) extends Task {
    override def getTaskDesc: String = {
      s"layout partition sanity ${layout.getId} $partition"
    }
  }

  sealed class PartitionSanityResult(val layout: Long, val partition: Long, val sanityCount: Long)

}
