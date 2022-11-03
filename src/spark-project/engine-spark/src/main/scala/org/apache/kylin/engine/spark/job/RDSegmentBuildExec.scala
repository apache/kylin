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
import io.kyligence.kap.engine.spark.job.RDSegmentBuildJob
import org.apache.hadoop.fs.Path
import org.apache.kylin.engine.spark.builder.SegmentFlatTable
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils

import java.io.IOException
import scala.collection.JavaConverters._

class RDSegmentBuildExec(private val jobContext: RDSegmentBuildJob, //
                         private val dataSegment: NDataSegment) extends Logging {
  // Resource detect segment build exec.

  // Needed variables from job context.
  protected final val jobId = jobContext.getJobId
  protected final val config = jobContext.getConfig
  protected final val dataflowId = jobContext.getDataflowId
  protected final val sparkSession = jobContext.getSparkSession
  protected final val rdSharedPath = jobContext.getRdSharedPath
  protected final val readOnlyLayouts = jobContext.getReadOnlyLayouts

  // Needed variables from data segment.
  protected final val segmentId = dataSegment.getId
  protected final val project = dataSegment.getProject

  private lazy val spanningTree = new AdaptiveSpanningTree(config, new AdaptiveTreeBuilder(dataSegment, readOnlyLayouts))

  private lazy val flatTableDesc = new SegmentFlatTableDesc(config, dataSegment, spanningTree)

  private lazy val flatTable = new SegmentFlatTable(sparkSession, flatTableDesc)

  @throws(classOf[IOException])
  def detectResource(): Unit = {

    val flatTableExecutions = if (spanningTree.fromFlatTable()) {
      Seq((-1L, flatTable.getFlatTablePartDS.queryExecution))
    } else {
      Seq.empty
    }

    val layoutExecutions = spanningTree.getRootNodes.asScala.map { node => //
      val layout = node.getLayout
      val execution = StorageStoreUtils.toDF(dataSegment, layout, sparkSession).queryExecution
      (layout.getId, execution)
    }

    val sourceSize = Maps.newHashMap[String, Long]()
    val sourceLeaves = Maps.newHashMap[String, Int]()

    (flatTableExecutions ++ layoutExecutions).foreach { case (parentId, execution) =>
      val sourceName = String.valueOf(parentId)
      val leaves = Integer.parseInt(ResourceDetectUtils.getPartitions(execution.executedPlan))
      logInfo(s"Leaf nodes: $leaves")
      val paths = ResourceDetectUtils.getPaths(execution.sparkPlan).map(_.toString).asJava
      logInfo(s"Detected source: $sourceName $leaves ${paths.asScala.mkString(",")}")
      val startTime = System.currentTimeMillis()
      logInfo(s"Detect source size start time is $startTime")
      val resourceSize = ResourceDetectUtils.getResourceSize(SparderEnv.getHadoopConfiguration(),config.isConcurrencyFetchDataSourceSize,
        paths.asScala.map(path => new Path(path)): _*)
      val endTime = System.currentTimeMillis()
      logInfo(s"Detect source size end time is $endTime")

      logInfo(s"Detect source size $resourceSize")
      sourceSize.put(sourceName, resourceSize)
      sourceLeaves.put(sourceName, leaves)
    }

    ResourceDetectUtils.write(new Path(rdSharedPath, //
      s"${segmentId}_${ResourceDetectUtils.fileName()}"), sourceSize)
    ResourceDetectUtils.write(new Path(rdSharedPath, //
      s"${segmentId}_${ResourceDetectUtils.cubingDetectItemFileSuffix}"), sourceLeaves)
  }
}
