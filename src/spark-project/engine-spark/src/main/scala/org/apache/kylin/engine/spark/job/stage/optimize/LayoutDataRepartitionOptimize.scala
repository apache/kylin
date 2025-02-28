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
import org.apache.hadoop.fs.Path
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.LayoutDataOptimizeJob
import org.apache.kylin.engine.spark.job.SegmentExec.RepartitionOptimizeResult
import org.apache.kylin.metadata.cube.model.NDataLayoutDetails
import org.apache.spark.sql.delta.DeltaLog

import scala.collection.JavaConverters._
import scala.concurrent.Future

import io.delta.tables.DeltaTable

class LayoutDataRepartitionOptimize(layoutDataOptimizeJob: LayoutDataOptimizeJob) extends LayoutOptimizeExec {
  override def getStageName: String = "layout data repartition"

  override def getJobContext: SparkApplication = layoutDataOptimizeJob

  override def execute(): Unit = {
    if (!canSkip) {
      layoutDataOptimizeJob.getLayoutDetails()
        .filter(_.getPartitionColumns != null)
        .filter(_.getPartitionColumns.size() > 0)
        .foreach(
          layoutDetail => {
            val repartitionFuture = Future {
              layoutRepartition(layoutDetail)
            }(OptimizeExecutionContext.futureExecutionContext)
            pipe.offer(repartitionFuture)
          })
      drain[RepartitionOptimizeResult]()
    } else {
      logInfo(s"Skip stage layout repartition optimize ${layoutDataOptimizeJob.dataFlow.getModel.getId}")
    }
  }

  private def layoutRepartition(layoutDetail: NDataLayoutDetails): RepartitionOptimizeResult = {
    val layoutTablePath = layoutDetail.getLocation
    val desc = s"Optimize repartition layout {${layoutDetail.getLayoutId}} by " +
      s"{${String.join(",", layoutDetail.getPartitionColumns)}}"
    OptimizeExecutionContext.setJobDesc(getJobContext.getSparkSession, desc)
    logInfo(s"Optimize repartition layout table [$layoutTablePath] by " +
      s"{${String.join(",", layoutDetail.getPartitionColumns)}}")
    val partitionCols = DeltaLog.forTable(getJobContext.getSparkSession, new Path(layoutTablePath))
      .snapshot.metadata.partitionColumns
    val existsCols = layoutDetail.getPartitionColumns.asScala
      .map(layoutDataOptimizeJob.getColNameByIdentity)
    if (!partitionCols.contains(existsCols)) {
      DeltaTable.forPath(layoutTablePath).toDF
        .write
        .partitionBy(layoutDetail.getPartitionColumns.asScala
          .map(layoutDataOptimizeJob.getColNameByIdentity): _*)
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(layoutTablePath)
    } else {
      logInfo(s"Skip repartition layout ${layoutTablePath}")
    }

    OptimizeExecutionContext.cancelJobDesc(getJobContext.getSparkSession)
    RepartitionOptimizeResult(layoutDetail.getLayoutId)
  }

  override def canSkip: Boolean = {
    layoutDataOptimizeJob.getConfig.isV3SkipRepartitionOptimize
  }
}
