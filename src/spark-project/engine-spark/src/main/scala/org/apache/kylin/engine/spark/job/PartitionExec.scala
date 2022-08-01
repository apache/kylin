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

import java.util
import java.util.Objects
import com.google.common.collect.{Lists, Maps}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.engine.spark.job.PartitionExec.PartitionResult
import org.apache.kylin.engine.spark.job.SegmentExec.ResultType
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.job.JobBucket
import org.apache.spark.sql.datasource.storage.{StorageListener, WriteTaskStats}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable

private[job] trait PartitionExec {
  this: SegmentExec =>

  protected val newBuckets: Seq[JobBucket]

  protected final lazy val partitions = {
    val distincted = newBuckets.map(_.getPartitionId).distinct.sorted
    logInfo(s"Segment $segmentId partitions: ${distincted.mkString("[", ",", "]")}")
    scala.collection.JavaConverters.seqAsJavaList(distincted.map(java.lang.Long.valueOf))
  }

  protected final lazy val partitionColumns = {
    val columns: mutable.Seq[Integer] = //
      dataModel.getMultiPartitionDesc.getColumnRefs.asScala.map { ref => //
        val id = dataModel.getColumnIdByColumnName(ref.getAliasDotName)
        require(id != -1, s"Couldn't find id of column ${ref.getAliasDotName} in model ${dataModel.getId}")
        Integer.valueOf(id)
      }
    logInfo(s"Partition column id [${columns.mkString(",")}]")
    columns
  }.toSet.asJava

  protected final def newLayoutPartition(dataSegment: NDataSegment, //
                                         layout: LayoutEntity, //
                                         partitionId: java.lang.Long, //
                                         layoutDS: Dataset[Row], //
                                         readableDesc: String,
                                         storageListener: Option[StorageListener]): Unit = {
    val newBucketId = newBuckets.filter(_.getLayoutId == layout.getId) //
      .filter(_.getPartitionId == partitionId) //
      .head.getBucketId
    logInfo(s"Layout partition bucket: ${layout.getId} $partitionId $newBucketId.")
    val storagePath = NSparkCubingUtil.getStoragePath(dataSegment, layout.getId, newBucketId)
    val taskStats = saveWithStatistics(layout, layoutDS, storagePath, readableDesc, storageListener)
    pipe.offer(PartitionResult(layout.getId, partitionId, newBucketId, taskStats))
  }

  override protected def wrapDimensions(layout: LayoutEntity): util.Set[Integer] = {
    // Implicitly included with multi level partition columns
    val dimensions = NSparkCubingUtil.combineIndices(partitionColumns, layout.getOrderedDimensions.keySet())
    logInfo(s"Layout dimensions: ${layout.getId} ${dimensions.asScala.mkString("[", ",", "]")}")
    dimensions
  }

  override protected def drain(): Unit = synchronized {
    var entry = pipe.poll()
    if (Objects.isNull(entry)) {
      return
    }
    val results = Lists.newArrayList(entry.asInstanceOf[PartitionResult])
    entry = pipe.poll()
    while (Objects.nonNull(entry)) {
      results.add(entry.asInstanceOf[PartitionResult])
      entry = pipe.poll()
    }
    logInfo(s"Segment $segmentId drained layout partition: " + //
      s"${results.asScala.map(lp => s"(${lp.layoutId} ${lp.partitionId})").mkString("[", ",", "]")}")
    class DFUpdate extends UnitOfWork.Callback[Int] {
      override def process(): Int = {
        // Merge into the newest data segment.
        val manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, project)
        val copiedDataflow = manager.getDataflow(dataflowId).copy()
        val copiedSegment = copiedDataflow.getSegment(segmentId)

        val dataLayouts = results.asScala.groupBy(_.layoutId).values.map { grouped =>
          val head = grouped.head
          val layoutId = head.layoutId
          val existedLayout = copiedSegment.getLayout(layoutId)
          val dataLayout = if (Objects.isNull(existedLayout)) {
            NDataLayout.newDataLayout(copiedDataflow, segmentId, layoutId)
          } else {
            existedLayout
          }

          val adds = Lists.newArrayList[LayoutPartition]()
          grouped.foreach { lr =>
            val taskStats = lr.stats
            val partitionId = lr.partitionId
            val existedPartition = dataLayout.getDataPartition(partitionId)
            val dataPartition = if (Objects.isNull(existedPartition)) {
              val newPartition = new LayoutPartition(partitionId)
              adds.add(newPartition)
              newPartition
            } else {
              existedPartition
            }
            // Job id should be set.
            dataPartition.setBuildJobId(jobId)
            dataPartition.setBucketId(lr.buckedId)
            dataPartition.setRows(taskStats.numRows)
            dataPartition.setSourceRows(taskStats.sourceRows)
            dataPartition.setFileCount(taskStats.numFiles)
            dataPartition.setByteSize(taskStats.numBytes)
          }
          dataLayout.getMultiPartition.addAll(adds)
          dataLayout.setBuildJobId(jobId)
          val partitions = dataLayout.getMultiPartition.asScala
          dataLayout.setRows(partitions.map(_.getRows).sum)
          dataLayout.setSourceRows(partitions.map(_.getSourceRows).sum)
          dataLayout.setFileCount(partitions.map(_.getFileCount).sum)
          dataLayout.setByteSize(partitions.map(_.getByteSize).sum)
          dataLayout
        }.toSeq

        updateDataLayouts(manager, dataLayouts)
      }
    }
    UnitOfWork.doInTransactionWithRetry(new DFUpdate, project)
  }

  protected final def newSegmentPartition(copiedSegment: NDataSegment, //
                                          partitionId: Long, //
                                          newAdds: java.util.List[SegmentPartition]): SegmentPartition = {
    val existed = copiedSegment.getPartition(partitionId)
    if (Objects.isNull(existed)) {
      val newPartition = new SegmentPartition(partitionId)
      newAdds.add(newPartition)
      newPartition
    } else {
      existed
    }
  }

  protected final def mergeSegmentStatistics(copiedSegment: NDataSegment): Unit = {
    val partitions = copiedSegment.getMultiPartitions.asScala
    copiedSegment.setSourceCount(partitions.map(_.getSourceCount).sum)
    val merged = Maps.newHashMap[String, java.lang.Long]()
    partitions.map(_.getColumnSourceBytes) //
      .foreach { item => //
        item.asScala.foreach { case (k, v) => //
          merged.put(k, v + merged.getOrDefault(k, 0L))
        }
      }
    copiedSegment.setColumnSourceBytes(merged)
  }

}

object PartitionExec {

  case class PartitionResult(layoutId: java.lang.Long, //
                             partitionId: java.lang.Long, //
                             buckedId: java.lang.Long, //
                             stats: WriteTaskStats) extends ResultType

}
