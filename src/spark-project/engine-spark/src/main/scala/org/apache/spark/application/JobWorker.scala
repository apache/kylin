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

import java.util.concurrent.Executors

import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.scheduler._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.KylinJobEventLoop

class JobWorker(application: SparkApplication, args: Array[String], eventLoop: KylinJobEventLoop) extends Logging {
  private val pool = Executors.newSingleThreadExecutor()

  def getApplication: SparkApplication = application

  eventLoop.registerListener(new KylinJobListener {
    override def onReceive(event: KylinJobEvent): Unit = {
      event match {
        case _: RunJob => runJob()
        case _ =>
      }
    }
  })

  def stop(): Unit = {
    pool.shutdownNow()
    application.logJobInfo()
  }

  private def runJob(): Unit = {
    execute()
  }


  private def execute(): Unit = {
    pool.execute(new Runnable {
      override def run(): Unit = {
        try {
          application.execute(args)
          eventLoop.post(JobSucceeded())
        } catch {
          case exception: NoRetryException => eventLoop.post(UnknownThrowable(exception))
          case throwable: Throwable => eventLoop.post(ResourceLack(throwable))
        }
      }
    })
  }
}

class NoRetryException(msg: String) extends java.lang.Exception(msg) {
  def this() {
    this(null)
  }
}
