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

import com.google.common.collect.Maps
import org.apache.kylin.engine.spark.builder.PartitionFlatTable
import org.apache.kylin.engine.spark.model.PartitionFlatTableDesc
import org.apache.kylin.metadata.cube.cuboid.PartitionSpanningTree
import org.apache.kylin.metadata.cube.cuboid.PartitionSpanningTree.{PartitionTreeBuilder, PartitionTreeNode}
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils

import java.io.IOException
import scala.collection.JavaConverters._

class RDPartitionBuildExec(private val jobContext: RDSegmentBuildJob, //
                           private val dataSegment: NDataSegment) extends RDSegmentBuildExec(jobContext, dataSegment) {

  private val newBuckets =
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq

  protected final lazy val partitions = {
    val distincted = newBuckets.map(_.getPartitionId).distinct.sorted
    logInfo(s"Segment $segmentId partitions: ${distincted.mkString("[", ",", "]")}")
    scala.collection.JavaConverters.seqAsJavaList(distincted.map(java.lang.Long.valueOf))
  }

  private lazy val spanningTree = new PartitionSpanningTree(config, //
    new PartitionTreeBuilder(dataSegment, readOnlyLayouts, jobId, partitions))

  private lazy val flatTableDesc = new PartitionFlatTableDesc(config, dataSegment, spanningTree, jobId, partitions)

  private lazy val flatTable = new PartitionFlatTable(sparkSession, flatTableDesc)


  @throws(classOf[IOException])
  override def detectResource(): Unit = {

    val flatTableExecutions = if (spanningTree.fromFlatTable()) {
      Seq((-1L, Seq(flatTable.getFlatTablePartDS.queryExecution)))
    } else {
      Seq.empty
    }

    val layoutExecutions = spanningTree.getRootNodes.asScala.map(_.asInstanceOf[PartitionTreeNode])
      .groupBy(_.getLayout.getId)
      .map { case (layoutId, grouped) => //
        val executions = grouped.map(node => //
          StorageStoreUtils.toDF(dataSegment, node.getLayout, node.getPartition, sparkSession).queryExecution)
          .toSeq
        (layoutId, executions)
      }.toSeq

    val sourceSize = Maps.newHashMap[String, Long]()
    val sourceLeaves = Maps.newHashMap[String, Int]()

    (flatTableExecutions ++ layoutExecutions).foreach { case (parentId, executions) =>
      val sourceName = String.valueOf(parentId)
      val leaves = executions.map(execution => //
        Integer.parseInt(ResourceDetectUtils.getPartitions(execution.executedPlan))).sum

      val paths = executions.flatMap(execution => //
        ResourceDetectUtils.getPaths(execution.sparkPlan).map(_.toString)
      ).asJava

      logInfo(s"Detected source: $sourceName $leaves ${paths.asScala.mkString(",")}")
      sourceSize.put(sourceName, ResourceDetectUtils.getResourceSize(SparderEnv.getHadoopConfiguration(),
        config.isConcurrencyFetchDataSourceSize, paths.asScala.map(path => new Path(path)): _*))
      sourceLeaves.put(sourceName, leaves)
    }

    ResourceDetectUtils.write(new Path(rdSharedPath, //
      s"${segmentId}_${ResourceDetectUtils.fileName()}"), sourceSize)
    ResourceDetectUtils.write(new Path(rdSharedPath, //
      s"${segmentId}_${ResourceDetectUtils.cubingDetectItemFileSuffix}"), sourceLeaves)
  }
}
