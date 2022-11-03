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

import io.kyligence.kap.engine.spark.job.SegmentBuildJob
import org.apache.commons.lang.StringUtils
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.stage.StageExec
import org.apache.kylin.engine.spark.job.{KylinBuildEnv, SegmentJob}
import org.apache.kylin.metadata.cube.model.NDataSegment

class RefreshSnapshots(jobContext: SegmentJob) extends StageExec {

  override def getJobContext: SparkApplication = jobContext

  override def getDataSegment: NDataSegment = null

  override def getSegmentId: String = null

  override def getId: String = {
    val jobStepId = StringUtils.replace(KylinBuildEnv.get().buildJobInfos.getJobStepId, SparkApplication.JOB_NAME_PREFIX, "")
    jobStepId + "_01"
  }

  override def execute(): Unit = {
    jobContext match {
      case job: SegmentBuildJob =>
        job.tryRefreshSnapshots(this)
      case _ =>
    }
  }
}
