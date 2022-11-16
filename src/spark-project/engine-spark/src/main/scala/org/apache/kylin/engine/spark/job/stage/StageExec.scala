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

  def getJobContext: SparkApplication

  def getDataSegment: NDataSegment

  def getSegmentId: String

  def getId: String = id

  def execute(): Unit

  def onStageStart(): Unit = {
    val taskId = getId
    val segmentId = getSegmentId
    val project = getJobContext.getProject
    val status = ExecutableState.RUNNING.toString
    val errMsg = null
    val updateInfo: util.HashMap[String, String] = null

    updateStageInfo(taskId, segmentId, project, status, errMsg, updateInfo)
  }

  def updateStageInfo(taskId: String, segmentId: String, project: String, status: String,
                      errMsg: String, updateInfo: util.HashMap[String, String]): Unit = {
    val context = getJobContext

    val url = "/kylin/api/jobs/stage/status"

    val payload: util.HashMap[String, Object] = new util.HashMap[String, Object](6)
    payload.put("task_id", taskId)
    payload.put("segment_id", segmentId)
    payload.put("project", project)
    payload.put("status", status)
    payload.put("err_msg", errMsg)
    payload.put("update_info", updateInfo)
    val json = JsonUtil.writeValueAsString(payload)
    val params = new util.HashMap[String, String]()
    val config = KylinConfig.getInstanceFromEnv
    params.put(ParamsConstants.TIME_OUT, config.getUpdateJobInfoTimeout.toString)
    params.put(ParamsConstants.JOB_TMP_DIR, config.getJobTmpDir(project, true))
    context.getReport.updateSparkJobInfo(params, url, json)
  }

  def onStageFinished(result: Boolean): Unit = {
    val taskId = getId
    val segmentId = getSegmentId
    val project = getJobContext.getProject
    val status = if (result) ExecutableState.SUCCEED.toString else ExecutableState.ERROR.toString
    val errMsg = null
    val updateInfo: util.HashMap[String, String] = null

    updateStageInfo(taskId, segmentId, project, status, errMsg, updateInfo)
  }

  def onBuildLayoutSuccess(layoutCount: Int): Unit = {
    val taskId = getId
    val segmentId = getSegmentId
    val project = getJobContext.getProject
    val status = null
    val errMsg = null
    val updateInfo: util.HashMap[String, String] = new util.HashMap[String, String]
    updateInfo.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, String.valueOf(layoutCount))

    updateStageInfo(taskId, segmentId, project, status, errMsg, updateInfo)
  }

  def onStageSkipped(): Unit = {
    val taskId = getId
    val segmentId = getSegmentId
    val project = getJobContext.getProject
    val status = ExecutableState.SKIP.toString
    val errMsg = null
    val updateInfo: util.HashMap[String, String] = null

    updateStageInfo(taskId, segmentId, project, status, errMsg, updateInfo)
  }

  def toWork(): Unit = {
    onStageStart()
    var result: Boolean = false
    try {
      execute()
      result = true
    } catch {
      case throwable: Throwable =>
        KylinBuildEnv.get().buildJobInfos.recordSegmentId(getSegmentId)
        KylinBuildEnv.get().buildJobInfos.recordStageId(getId)
        Throwables.propagate(throwable)
    } finally onStageFinished(result)
  }

  def toWorkWithoutFinally(): Unit = {
    onStageStart()
    try {
      execute()
    } catch {
      case throwable: Throwable =>
        KylinBuildEnv.get().buildJobInfos.recordSegmentId(getSegmentId)
        KylinBuildEnv.get().buildJobInfos.recordStageId(getId)
        Throwables.propagate(throwable)
    }
  }


  def setId(id: String): Unit = {
    this.id = id
  }
}
