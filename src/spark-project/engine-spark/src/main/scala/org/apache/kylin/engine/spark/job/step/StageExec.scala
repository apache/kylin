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

package org.apache.kylin.engine.spark.job.step

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.JsonUtil
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.{KylinBuildEnv, NSparkExecutable, ParamsConstants}
import org.apache.kylin.guava30.shaded.common.base.Throwables
import org.apache.kylin.guava30.shaded.common.util.concurrent.RateLimiter
import org.apache.kylin.job.execution.ExecutableState
import org.apache.spark.internal.Logging

import java.util

trait StageExec extends Logging {

  protected var stageId: String = _

  def getStageName: String

  def getJobContext: SparkApplication

  def getSegmentId: String = null

  def execute(): Unit

  def createRateLimiter(permitsPerSecond: Double = 0.1): RateLimiter = {
    RateLimiter.create(permitsPerSecond)
  }

  def onStageStart(): Unit = {
    if(getJobContext.isSkipFollowingStages(getSegmentId)){
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

  def updateStageInfo(status: String, errMsg: String, updateInfo: util.Map[String, String]): Unit = {
    val context = getJobContext

    val url = "/kylin/api/jobs/stage/status"

    val payload: util.HashMap[String, Object] = new util.HashMap[String, Object](6)
    payload.put("task_id", getStageId)
    payload.put("segment_id", getSegmentId)
    payload.put("project", context.getProject)
    payload.put("status", status)
    payload.put("err_msg", errMsg)
    payload.put("update_info", updateInfo)
    payload.put("job_last_running_start_time", context.getParam(NSparkExecutable.JOB_LAST_RUNNING_START_TIME))
    val json = JsonUtil.writeValueAsString(payload)
    val params = new util.HashMap[String, String]()
    val config = KylinConfig.getInstanceFromEnv
    params.put(ParamsConstants.TIME_OUT, config.getUpdateJobInfoTimeout.toString)
    params.put(ParamsConstants.JOB_TMP_DIR, config.getJobTmpDir(context.getProject, true))
    context.getReport.updateSparkJobInfo(params, url, json)
  }

  def getStageId: String = stageId

  def setStageId(stageId: String): Unit = {
    this.stageId = stageId
  }

  def doExecute(): Unit = {
    doExecute0()
  }

  def doExecuteWithoutFinally(): Unit = {
    doExecute0(false)
  }

  private def doExecute0(doFinally: Boolean = true): Unit = {
    logInfo(s"Sub step start: $getStageName")
    onStageStart()
    var state: ExecutableState = ExecutableState.SUCCEED
    try {
      if(getJobContext.isSkipFollowingStages(getSegmentId)){
        state = ExecutableState.SKIP
        return
      }
      execute()
    } catch {
      case t: Throwable =>
        logError(s"An exception occurred during the build stage[${this.getClass.getName}] : $t")
        state = ExecutableState.ERROR
        KylinBuildEnv.get().buildJobInfos.recordSegmentId(getSegmentId)
        KylinBuildEnv.get().buildJobInfos.recordStageId(getStageId)
        Throwables.propagate(t)
    } finally {
      if (doFinally) {
        onStageFinished(state)
      }
    }
    logInfo(s"Sub step end: $getStageName")
  }
}
