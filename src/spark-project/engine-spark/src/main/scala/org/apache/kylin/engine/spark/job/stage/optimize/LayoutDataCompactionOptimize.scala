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

package org.apache.kylin.engine.spark.job.stage.optimize

import org.apache.commons.lang3.StringUtils
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.LayoutDataOptimizeJob
import org.apache.kylin.engine.spark.job.SegmentExec.CompactionOptimizeResult
import org.apache.kylin.metadata.cube.model.NDataLayoutDetails

import scala.concurrent.Future

import io.delta.tables.DeltaTable

class LayoutDataCompactionOptimize(layoutDataOptimizeJob: LayoutDataOptimizeJob) extends LayoutOptimizeExec {

  override def getStageName: String = "layout data compaction"

  override def getJobContext: SparkApplication = layoutDataOptimizeJob

  override def execute(): Unit = {
    if (!canSkip) {
      layoutDataOptimizeJob.getLayoutDetails()
        .filter(_.isCompactionAfterUpdate)
        .foreach(layoutDetail => {
          val repartitionFuture = Future {
            compactionLayout(layoutDetail)
          }(OptimizeExecutionContext.futureExecutionContext)
          pipe.offer(repartitionFuture)
        })
      drain[CompactionOptimizeResult]()
    } else {
      logInfo(s"Skip stage layout compaction optimize ${layoutDataOptimizeJob.dataFlow.getModel.getId}")
    }
  }

  private def compactionLayout(layoutDetail: NDataLayoutDetails): CompactionOptimizeResult = {
    val layoutTablePath = layoutDetail.getLocation
    if (layoutDetail.getMaxCompactionFileSizeInBytes > 0) {
      getJobContext.getSparkSession.sessionState.conf.setLocalProperty("spark.databricks.delta.optimize.maxFileSize",
        layoutDetail.getMaxCompactionFileSizeInBytes.toString)
    }
    if (layoutDetail.getMinCompactionFileSizeInBytes > 0) {
      getJobContext.getSparkSession.sessionState.conf.setLocalProperty("spark.databricks.delta.optimize.minFileSize",
        layoutDetail.getMinCompactionFileSizeInBytes.toString)
    }

    val desc = s"Optimize compaction for layout {${layoutDetail.getLayoutId}}"
    logInfo(desc)
    OptimizeExecutionContext.setJobDesc(getJobContext.getSparkSession, desc)
    DeltaTable.forPath(layoutTablePath).optimize().executeCompaction()

    OptimizeExecutionContext.cancelJobDesc(getJobContext.getSparkSession)
    CompactionOptimizeResult(layoutDetail.getLayoutId)
  }

  override def canSkip: Boolean = {
    layoutDataOptimizeJob.getConfig.isV3SkipCompactionOptimize
  }

}
