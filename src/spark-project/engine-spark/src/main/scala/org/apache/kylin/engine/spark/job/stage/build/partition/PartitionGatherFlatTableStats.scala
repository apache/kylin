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

import org.apache.kylin.guava30.shaded.common.collect.Queues
import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase.Statistics
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._

class PartitionGatherFlatTableStats(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends PartitionBuildStage(jobContext, dataSegment, buildParam) {

  private lazy val statsResultQueue = Queues.newLinkedBlockingDeque[PartitionStatsResult]()

  override def execute(): Unit = {
    val spanningTree = buildParam.getPartitionSpanningTree
    val flatTable = buildParam.getPartitionFlatTable
    if (spanningTree.fromFlatTable()) {
      // Very very heavy step
      // Potentially global dictionary building & encoding within.
      // Materialize flat table.
      // flatTable.getFlatTableDS

      // Collect partitions' flat table dataset and statistics.
      logInfo(s"Segment $segmentId collect partitions' flat table dataset and statistics.")

      val taskIter = spanningTree.getFlatTablePartitions.asScala.map(new PartitionStatsTask(_)).iterator

      slowStartExec(taskIter, (statsTask: PartitionStatsTask) => {
        val partition = statsTask.partition
        val partitionDS = flatTable.getPartitionDS(partition)
        val stats = buildPartitionStatistics(partition, partitionDS)
        statsResultQueue.offer(new PartitionStatsResult(partition, partitionDS, stats))
      })

      val collected = polledResultSeq(statsResultQueue)
      buildParam.setCachedPartitionDS(collected.map(r => (r.partition, r.partitionDS)).toMap)
      buildParam.setCachedPartitionStats(collected.map(r => (r.partition, r.stats)).toMap)

      logInfo(s"Segment $segmentId finished collect partitions' " +
        s"flat table dataset and statistics ${buildParam.getCachedPartitionStats}.")
    }

    // Build root node's layout partition sanity cache.
    buildSanityCache()
  }

  sealed class PartitionStatsResult(val partition: Long,
                                    val partitionDS: Dataset[Row],
                                    val stats: Statistics)

  sealed class PartitionStatsTask(val partition: Long) extends Task {
    override def getTaskDesc: String = {
      s"partition stats $partition"
    }
  }

  override def getStageName: String = "PartitionGatherFlatTableStats"
}
