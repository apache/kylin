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

package org.apache.kylin.engine.spark.job.stage.build

import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.metadata.cube.model.NDataSegment

import scala.collection.JavaConverters._

class GatherFlatTableStats(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends BuildStage(jobContext, dataSegment, buildParam) {

  override def execute(): Unit = {
    // Build flat table?
    if (buildParam.getSpanningTree.fromFlatTable()) {
      // Collect statistics for flat table.
      val statistics = buildStatistics()
      buildParam.setFlatTableStatistics(statistics)

      // Build inferior flat table.
      if (config.isInferiorFlatTableEnabled) {
        buildInferior()
      }
    }

    // Build root node's layout sanity cache.
    buildSanityCache()

    // Cleanup previous potentially left temp layout data.
    cleanupLayoutTempData(dataSegment, readOnlyLayouts.asScala.toSeq)
  }

  override def getStageName: String = "GatherFlatTableStats"
}
