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

package org.apache.kylin.engine.spark.job.stage.merge.partition

import java.util.Objects

import org.apache.kylin.guava30.shaded.common.collect.Lists
import org.apache.kylin.engine.spark.job.stage.merge.MergeStage
import org.apache.kylin.engine.spark.job.{PartitionExec, SegmentJob}
import org.apache.kylin.metadata.cube.model._
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.common.persistence.transaction.UnitOfWork.Callback
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._

abstract class PartitionMergeStage(private val jobContext: SegmentJob,
                                   private val dataSegment: NDataSegment)
  extends MergeStage(jobContext, dataSegment) with PartitionExec {
  protected final val newBuckets = //
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq

  // Multi level partition FLAT-TABLE is not reusable.
  override protected def getUnmergedFTPaths: Seq[Path] = Seq.empty[Path]

  private case class PartitionMergeTask(grouped: Seq[(NDataLayout, LayoutPartition)]) extends Task {
    override def getTaskDesc: String = {
      val item = grouped.head
      s"layout ${item._1.getLayoutId} partition ${item._2.getPartitionId}"
    }
  }

  override protected def mergeIndices(): Unit = {
    val tasks = unmerged.flatMap(segment =>
      segment.getSegDetails.getEffectiveLayouts.asScala.flatMap(layout =>
        layout.getMultiPartition.asScala.map(partition => (layout, partition))
      )).groupBy(tp => (tp._1.getLayoutId, tp._2.getPartitionId)).values.map(PartitionMergeTask)
    slowStartExec(tasks.iterator, mergePartition)
  }

  private def mergePartition(task: PartitionMergeTask): Unit = {
    val head = task.grouped.head
    val layout = head._1.getLayout
    val partition = head._2
    val layoutId = layout.getId
    val partitionId = partition.getPartitionId
    val unitedDS: Dataset[Row] = newUnitedDS(partitionId, layoutId)
    if (Objects.isNull(unitedDS)) {
      return
    }

    mergeLayoutPartition(partitionId, layout, unitedDS)
  }

  private def newUnitedDS(partitionId: java.lang.Long, layoutId: java.lang.Long): Dataset[Row] = {
    var unitedDS: Dataset[Row] = null
    unmerged.foreach { segment =>
      val dataLayout = segment.getLayout(layoutId)
      if (Objects.isNull(dataLayout)) {
        logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Layout not found in segment, layout $layoutId segment ${segment.getId}")
      } else {
        val dataPartition = dataLayout.getDataPartition(partitionId)
        if (Objects.isNull(dataPartition)) {
          logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Partition not found in segment," +
            s" partition $partitionId layout $layoutId segment ${segment.getId}")
        } else {
          val layout = dataLayout.getLayout
          val partitionDS = StorageStoreUtils.toDF(segment, layout, partitionId, sparkSession)
          unitedDS = if (Objects.isNull(unitedDS)) {
            partitionDS
          } else {
            unitedDS.union(partitionDS)
          }
        }
      }
    }
    unitedDS
  }

  private def mergeLayoutPartition(partitionId: java.lang.Long, layout: LayoutEntity, unitedDS: Dataset[Row]): Unit = {
    val readableDesc = s"Merge layout ${layout.getId} partition $partitionId"
    val layoutDS = wrapLayoutDS(layout, unitedDS)
    newLayoutPartition(dataSegment, layout, partitionId, layoutDS, readableDesc, None)
  }

  override protected def mergeColumnBytes(): Unit = {
    UnitOfWork.doInTransactionWithRetry(new Callback[Unit] {
      override def process(): Unit = {
        val dataflowManager = NDataflowManager.getInstance(config, project)
        val copiedDataflow = dataflowManager.getDataflow(dataflowId).copy()
        val copiedSegment = copiedDataflow.getSegment(segmentId)
        val dataflowUpdate = new NDataflowUpdate(dataflowId)
        val newAdds = Lists.newArrayList[SegmentPartition]()
        unmerged.flatMap(_.getMultiPartitions.asScala) //
          .groupBy(_.getPartitionId) //
          .values.foreach { grouped => //
          val partitionId = grouped.head.getPartitionId
          val totalCount = grouped.map(_.getSourceCount).sum
          val evaluated = grouped.flatMap(_.getColumnSourceBytes.asScala) //
            .groupBy(_._1) //
            .mapValues(_.map(_._2).reduce(_ + _))
            .map { case (k, v) => k -> long2Long(v) }
            .asJava

          val segmentPartition = newSegmentPartition(copiedSegment, partitionId, newAdds)
          segmentPartition.setSourceCount(totalCount)
          segmentPartition.getColumnSourceBytes.putAll(evaluated)
        }
        copiedSegment.getMultiPartitions.addAll(newAdds)
        mergeSegmentStatistics(copiedSegment)
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
