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

package org.apache.kylin.engine.spark.job.stage

import com.google.common.base.Throwables
import io.kyligence.kap.guava20.shaded.common.util.concurrent.RateLimiter
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.JsonUtil
import org.apache.kylin.job.execution.ExecutableState
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.{KylinBuildEnv, ParamsConstants}
import org.apache.kylin.metadata.cube.model.{NBatchConstants, NDataSegment}
import org.apache.spark.internal.Logging

import java.util

trait StageExec extends Logging {
  protected var id: String = _

  def getStageName: String

  def getJobContext: SparkApplication

  def getDataSegment: NDataSegment

  def getSegmentId: String

  def getId: String = id

  def execute(): Unit

  def createRateLimiter(permitsPerSecond: Double = 0.1): RateLimiter = {
    RateLimiter.create(permitsPerSecond)
  }

  def onStageStart(): Unit = {
    if (getJobContext.isSkipFollowingStages(getSegmentId)) {
      return
    }
    updateStageInfo(ExecutableState.RUNNING.toString, null, null)
  }

  def onStageFinished(state: ExecutableState = ExecutableState.SUCCEED): Unit = {
    updateStageInfo(state.toString, null, null)
  }

  def onStageSkipped(): Unit = {
    updateStageInfo(ExecutableState.SKIP.toString, null, null)
  }

  def toWork(): Unit = {
    toWork0()
  }

  def toWorkWithoutFinally(): Unit = {
    toWork0(false)
  }

  def toWork0(doFinally: Boolean = true): Unit = {
    onStageStart()
    var state: ExecutableState = ExecutableState.SUCCEED
    try {
      if (getJobContext.isSkipFollowingStages(getSegmentId)) {
        state = ExecutableState.SKIP
        return
      }
      execute()
    } catch {
      case throwable: Throwable =>
        state = ExecutableState.ERROR
        KylinBuildEnv.get().buildJobInfos.recordSegmentId(getSegmentId)
        KylinBuildEnv.get().buildJobInfos.recordStageId(getId)
        Throwables.propagate(throwable)
    } finally {
      if (doFinally) {
        onStageFinished(state)
      }
    }
  }

  def updateStageInfo(status: String, errMsg: String, updateInfo: util.Map[String, String]): Unit = {
    val context = getJobContext

    val url = "/kylin/api/jobs/stage/status"

    val payload: util.HashMap[String, Object] = new util.HashMap[String, Object](6)
    payload.put("task_id", getId)
    payload.put("segment_id", getSegmentId)
    payload.put("project", context.getProject)
    payload.put("status", status)
    payload.put("err_msg", errMsg)
    payload.put("update_info", updateInfo)
    val json = JsonUtil.writeValueAsString(payload)
    val params = new util.HashMap[String, String]()
    val config = KylinConfig.getInstanceFromEnv
    params.put(ParamsConstants.TIME_OUT, config.getUpdateJobInfoTimeout.toString)
    params.put(ParamsConstants.JOB_TMP_DIR, config.getJobTmpDir(context.getProject, true))
    context.getReport.updateSparkJobInfo(params, url, json)
  }

  def setId(id: String): Unit = {
    this.id = id
  }
}
