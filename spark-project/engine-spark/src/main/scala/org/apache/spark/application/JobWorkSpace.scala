/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.application

import java.util
import java.util.concurrent.CountDownLatch

import io.kyligence.kap.engine.spark.application.SparkApplication
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.scheduler._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.KylinJobEventLoop

object JobWorkSpace extends Logging {
  def execute(args: Array[String]): Unit = {
    try {
      val (application, appArgs) = resolveArgs(args)
      val eventLoop = new KylinJobEventLoop
      val worker = new JobWorker(application, appArgs, eventLoop)
      val monitor = new JobMonitor(eventLoop)
      val workspace = new JobWorkSpace(eventLoop, monitor, worker)
      System.exit(workspace.run())
    } catch {
      case throwable: Throwable =>
        logError("Error occurred when init job workspace.", throwable)
        System.exit(1)
    }
  }

  def resolveArgs(args: Array[String]): (SparkApplication, Array[String]) = {
    logInfo("JobWorkSpace args:" + args.mkString(" "))
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
}
