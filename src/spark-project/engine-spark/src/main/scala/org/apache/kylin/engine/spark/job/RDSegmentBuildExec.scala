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

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase
import org.apache.kylin.guava30.shaded.common.collect.Maps
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils

import scala.collection.JavaConverters._

class RDSegmentBuildExec(private val jobContext: SegmentJob, //
                         private val dataSegment: NDataSegment, private val buildParam: BuildParam
                        )
  extends FlatTableAndDictBase(jobContext, dataSegment, buildParam) {
  // Resource detect segment build exec.

  protected final val rdSharedPath = jobContext.getRdSharedPath

  @throws(classOf[IOException])
  def detectResource(): Unit = {
    initFlatTableOnDetectResource()

    val flatTableExecutions = if (spanningTree.fromFlatTable()) {
      Seq((-1L, getFlatTablePartDS.queryExecution))
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
      val paths = ResourceDetectUtils.getPaths(execution.sparkPlan, true).map(_.toString).asJava
      logInfo(s"Detected source: $sourceName $leaves ${paths.asScala.mkString(",")}")
      val startTime = System.currentTimeMillis()
      val resourceSize = ResourceDetectUtils.getResourceSize(config, SparderEnv.getHadoopConfiguration(),
        paths.asScala.map(path => new Path(path)): _*)
      val endTime = System.currentTimeMillis()
      logInfo(s"Detect source size cost time is ${endTime - startTime} ms.")

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
