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

import java.util
import java.util.concurrent.CountDownLatch

import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.scheduler._
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
      if (System.getProperty("spark.master").equals("yarn") && System.getProperty("spark.submit.deployMode").equals("cluster")) {
        workspace.run()
      } else {
        System.exit(workspace.run())
      }
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
