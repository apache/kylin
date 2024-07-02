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
import org.apache.kylin.engine.spark.job.SegmentExec.VacuumOptimizeResult
import org.apache.kylin.metadata.cube.model.NDataLayoutDetails

import scala.concurrent.Future

import io.delta.tables.DeltaTable

class LayoutDataVacuumOptimize(layoutDataOptimizeJob: LayoutDataOptimizeJob) extends LayoutOptimizeExec {

  override def getStageName: String = "delete useless layout data"

  override def getJobContext: SparkApplication = layoutDataOptimizeJob

  override def onStageStart(): Unit = {
    super.onStageStart()
    layoutDataOptimizeJob.getSparkSession.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
  }

  override def execute(): Unit = {
    if (!canSkip) {
      layoutDataOptimizeJob.getLayoutDetails()
        .foreach(layoutDetail => {
          if (StringUtils.isNotEmpty(layoutDetail.getRangeFilterExpr)) {
            val vacuumFuture = Future {
              vacuumLayout(layoutDetail)
            }(OptimizeExecutionContext.futureExecutionContext)
            pipe.offer(vacuumFuture)
          }
        })
      drain[VacuumOptimizeResult]()
    } else {
      logInfo(s"Skip stage vacuum layout ${layoutDataOptimizeJob.dataFlow.getModel.getId}")
    }
  }

  private def vacuumLayout(layoutDetail: NDataLayoutDetails): VacuumOptimizeResult = {
    val desc = s"Vacuum layout ${layoutDetail.getLayoutId}"
    OptimizeExecutionContext.setJobDesc(getJobContext.getSparkSession, desc)

    val deltaTable = DeltaTable.forPath(layoutDetail.getLocation)
    val condition = s"not (${layoutDetail.getRangeFilterExpr})"

    logInfo(s"Start to delete and vacuum with condition $condition for $layoutDetail.getLocation")
    deltaTable.delete(condition)
    logInfo(s"Finished delete and vacuum with condition $condition for $layoutDetail.getLocation")

    OptimizeExecutionContext.cancelJobDesc(getJobContext.getSparkSession)
    VacuumOptimizeResult(layoutDetail.getLayoutId)
  }

  override def canSkip: Boolean = {
    layoutDataOptimizeJob.getConfig.isV3SkipVacuumOptimize
  }
}
