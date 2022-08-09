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

import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.scheduler._
import io.netty.util.internal.ThrowableUtil
import org.apache.kylin.common.util.Unsafe
import org.apache.spark.autoheal.ExceptionTerminator
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.KylinJobEventLoop

class JobMonitor(eventLoop: KylinJobEventLoop) extends Logging {
  var retryTimes = 0
  eventLoop.registerListener(new KylinJobListener {
    override def onReceive(event: KylinJobEvent): Unit = {
      event match {
        case rl: ResourceLack => handleResourceLack(rl)
        case ut: UnknownThrowable => handleUnknownThrowable(ut)
        case emr: ExceedMaxRetry => handleExceedMaxRetry(emr)
        case _ =>
      }
    }
  })

  def stop(): Unit = {
  }

  def handleResourceLack(rl: ResourceLack): Unit = {
    try {
      if (rl.throwable != null && rl.throwable.getCause != null) {
        if (rl.throwable.getCause.isInstanceOf[NoRetryException]) {
          eventLoop.post(JobFailed(rl.throwable.getCause.getMessage, rl.throwable.getCause))
          return
        }
      }

      logInfo(s"handleResourceLack --> ${rl.throwable.getCause}")
      val buildEnv = KylinBuildEnv.get()
      // if killed, job failed without retry
      val jobStepId = buildEnv.buildJobInfos.getJobStepId
      if (buildEnv.clusterManager.isApplicationBeenKilled(jobStepId)) {
        eventLoop.post(JobFailed(s"Submitted application $jobStepId has been killed.", rl.throwable))
        return
      }
      retryTimes += 1
      KylinBuildEnv.get().buildJobInfos.recordRetryTimes(retryTimes)
      val maxRetry = buildEnv.kylinConfig.getSparkEngineMaxRetryTime
      if (retryTimes <= maxRetry) {
        logError(s"Job failed $retryTimes time(s). Cause:", rl.throwable)
        Unsafe.setProperty("kylin.spark-conf.auto-prior", "false")
        ExceptionTerminator.resolveException(rl, eventLoop)
      } else {
        eventLoop.post(ExceedMaxRetry(rl.throwable))
      }
    } catch {
      case throwable: Throwable => eventLoop.post(JobFailed("Error occurred when generate retry configuration.", throwable))
    }
  }


  def handleExceedMaxRetry(emr: ExceedMaxRetry): Unit = {
    eventLoop.post(JobFailed("Retry times exceed MaxRetry set in the KylinConfig.", emr.throwable))
  }

  def handleUnknownThrowable(ur: UnknownThrowable): Unit = {
    eventLoop.post(JobFailed("Unknown error occurred during the job.", ur.throwable))
  }
}

case class RetryInfo(overrideConf: java.util.Map[String, String], throwable: Throwable) {
  override def toString: String = {
    s"""RetryInfo{
       |    overrideConf : $overrideConf,
       |    throwable : ${ThrowableUtil.stackTraceToString(throwable)}
       |}""".stripMargin
  }
}
