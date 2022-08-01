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

package org.apache.spark.application

import io.kyligence.kap.engine.spark.job.ParamsConstants
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kylin.common.util.{JsonUtil, Unsafe}
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.scheduler._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.KylinJobEventLoop

import java.util
import java.util.concurrent.CountDownLatch

/**
 * Spark driver part, construct the real spark job [SparkApplication]
 */
object JobWorkSpace extends Logging {
  def execute(args: Array[String]): Unit = {
    try {
      val (application, appArgs) = resolveArgs(args)
      val eventLoop = new KylinJobEventLoop
      val worker = new JobWorker(application, appArgs, eventLoop)
      val monitor = new JobMonitor(eventLoop)
      val workspace = new JobWorkSpace(eventLoop, monitor, worker)
      val statusCode = workspace.run()
      if (statusCode != 0) {
        Unsafe.systemExit(statusCode)
      }
    } catch {
      case throwable: Throwable =>
        logError("Error occurred when init job workspace.", throwable)
        Unsafe.systemExit(1)
    }
  }

  def resolveArgs(args: Array[String]): (SparkApplication, Array[String]) = {
    if (args.length < 2 || args(0) != "-className") throw new IllegalArgumentException("className is required")
    val className = args(1)
    // scalastyle:off
    val o = Class.forName(className).newInstance
    // scalastyle:on
    if (!o.isInstanceOf[SparkApplication]) throw new IllegalArgumentException(className + " is not a subClass of AbstractApplication")
    val appArgs = args.slice(2, args.length)
    val application = o.asInstanceOf[SparkApplication]
    (application, appArgs)
  }
}

class JobWorkSpace(eventLoop: KylinJobEventLoop, monitor: JobMonitor, worker: JobWorker) extends Logging {
  require(eventLoop != null)
  require(monitor != null)
  require(worker != null)

  private var statusCode: Int = 0
  private val latch = new CountDownLatch(1)

  eventLoop.registerListener(new KylinJobListener {
    override def onReceive(event: KylinJobEvent): Unit = {
      event match {
        case _: JobSucceeded => success()
        case jf: JobFailed => fail(jf)
        case _ =>
      }
    }
  })

  def run(): Int = {
    eventLoop.start()
    eventLoop.post(RunJob())
    latch.await()
    statusCode
  }

  def success(): Unit = {
    try {
      stop()
    } finally {
      statusCode = 0
      latch.countDown()
    }
  }

  def fail(jf: JobFailed): Unit = {
    try {
      logError(s"Job failed eventually. Reason: ${jf.reason}", jf.throwable)
      KylinBuildEnv.get().buildJobInfos.recordJobRetryInfos(RetryInfo(new util.HashMap, jf.throwable))
      updateJobErrorInfo(jf)
      stop()
    } finally {
      statusCode = 1
      latch.countDown()
    }
  }

  def stop(): Unit = {
    monitor.stop()
    worker.stop()
    eventLoop.stop()
  }

  def updateJobErrorInfo(jf: JobFailed): Unit = {
    val infos = KylinBuildEnv.get().buildJobInfos
    val context = worker.getApplication

    val project = context.getProject
    val jobId = context.getJobId

    val stageId = infos.getStageId
    val jobStepId = StringUtils.replace(infos.getJobStepId, SparkApplication.JOB_NAME_PREFIX, "")
    val failedStepId = if (StringUtils.isBlank(stageId)) jobStepId else stageId

    val failedSegmentId = infos.getSegmentId
    val failedStack = ExceptionUtils.getStackTrace(jf.throwable)
    val failedReason =
      if (context.getAtomicUnreachableSparkMaster.get()) "Unable connect spark master to reach timeout maximum time"
      else jf.reason
    val url = "/kylin/api/jobs/error"

    val payload: util.HashMap[String, Object] = new util.HashMap[String, Object](5)
    payload.put("project", project)
    payload.put("job_id", jobId)
    payload.put("failed_step_id", failedStepId)
    payload.put("failed_segment_id", failedSegmentId)
    payload.put("failed_stack", failedStack)
    payload.put("failed_reason", failedReason)
    val json = JsonUtil.writeValueAsString(payload)
    val params = new util.HashMap[String, String]()
    val config = KylinBuildEnv.get().kylinConfig
    params.put(ParamsConstants.TIME_OUT, config.getUpdateJobInfoTimeout.toString)
    params.put(ParamsConstants.JOB_TMP_DIR, config.getJobTmpDir(project, true))
    context.getReport.updateSparkJobInfo(params, url, json);
  }
}
