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

package org.apache.kylin.engine.spark.scheduler

import org.apache.kylin.engine.spark.utils.ThreadUtils

import java.util.concurrent.TimeUnit

class JobRuntime(val maxThreadCount: Int) {

  private lazy val minThreads = 1
  private lazy val maxThreads = Math.max(minThreads, maxThreadCount)
  // Maybe we should parameterize nThreads.
  private lazy val threadPool = //
    ThreadUtils.newDaemonScalableThreadPool("build-thread", //
      minThreads, maxThreads, 20, TimeUnit.SECONDS)

  // Drain layout result using single thread.
  private lazy val scheduler = //
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("build-scheduler")

  def submit(fun: () => Unit): Unit = {
    threadPool.submit(new Runnable {
      override def run(): Unit = fun.apply()
    })
  }

  def scheduleCheckpoint(fun: () => Unit): Unit = {
    scheduler.scheduleWithFixedDelay(() => fun.apply(), 60L, 60L, TimeUnit.SECONDS)
  }

  def schedule(func: () => Unit, delay: Long, unit: TimeUnit): Unit = {
    scheduler.schedule(new Runnable {
      override def run(): Unit = func.apply()
    }, delay, unit)
  }

  def shutdown(): Unit = {
    scheduler.shutdownNow()
    threadPool.shutdownNow()
  }

}
