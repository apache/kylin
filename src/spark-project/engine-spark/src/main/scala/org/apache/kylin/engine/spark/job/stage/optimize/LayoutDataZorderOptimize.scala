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
import org.apache.kylin.engine.spark.job.SegmentExec.ZorderOptimizeResult
import org.apache.kylin.metadata.cube.model.NDataLayoutDetails

import scala.collection.JavaConverters._
import scala.concurrent.Future

import io.delta.tables.DeltaTable

class LayoutDataZorderOptimize(layoutDataOptimizeJob: LayoutDataOptimizeJob) extends LayoutOptimizeExec {
  override def getStageName: String = "layout data zorder"

  override def getJobContext: SparkApplication = layoutDataOptimizeJob

  override def execute(): Unit = {
    if (!canSkip) {
      layoutDataOptimizeJob.getLayoutDetails()
        .filter(_.getZorderByColumns != null)
        .filter(_.getZorderByColumns.size() > 0)
        .foreach(layoutDetail => {
          val repartitionFuture = Future {
            zorderLayout(layoutDetail)
          }(OptimizeExecutionContext.futureExecutionContext)
          pipe.offer(repartitionFuture)
        })
      drain[ZorderOptimizeResult]()
    } else {
      logInfo(s"Skip stage optimize zorder layout ${layoutDataOptimizeJob.dataFlow.getModel.getId}")
    }
  }

  def zorderLayout(layoutDetail: NDataLayoutDetails): ZorderOptimizeResult = {
    val layoutTablePath = layoutDetail.getLocation
    val desc = s"Optimize zorder for layout table [$layoutTablePath] by " +
      s"{${String.join(",", layoutDetail.getZorderByColumns)}}"
    OptimizeExecutionContext.setJobDesc(getJobContext.getSparkSession, desc)
    logInfo(desc)

    DeltaTable.forPath(layoutTablePath).optimize()
      .executeZOrderBy(layoutDetail.getZorderByColumns.asScala
        .map(layoutDataOptimizeJob.getColNameByIdentityWithBackTick): _*)

    OptimizeExecutionContext.cancelJobDesc(getJobContext.getSparkSession)
    ZorderOptimizeResult(layoutDetail.getLayoutId)
  }

  override def canSkip: Boolean = {
    layoutDataOptimizeJob.getConfig.isV3SkipZorderOptimize
  }

}
