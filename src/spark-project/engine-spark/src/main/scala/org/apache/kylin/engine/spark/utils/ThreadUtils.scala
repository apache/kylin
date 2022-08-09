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

package org.apache.kylin.engine.spark.utils

import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.util.concurrent._

object ThreadUtils {

  private val NAME_SUFFIX: String = "-%d"

  def newDaemonThreadFactory(nameFormat: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build()
  }

  def newDaemonScalableThreadPool(prefix: String,
                                  corePoolSize: Int, //
                                  maximumPoolSize: Int, //
                                  keepAliveTime: Long, //
                                  unit: TimeUnit): ThreadPoolExecutor = {
    // Why not general BlockingQueue like LinkedBlockingQueue?
    // If there are more than corePoolSize but less than maximumPoolSize threads running,
    //  a new thread will be created only if the queue is full.
    // If we use unbounded queue, then maximumPoolSize will never be used.
    val queue = new LinkedTransferQueue[Runnable]() {
      override def offer(r: Runnable): Boolean = tryTransfer(r)
    }
    val factory = newDaemonThreadFactory(prefix + NAME_SUFFIX)
    val threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, //
      keepAliveTime, unit, queue, factory)
    threadPool.setRejectedExecutionHandler(new RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = try {
        executor.getQueue.put(r)
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    })
    threadPool
  }

  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val factory = newDaemonThreadFactory(threadName)
    val executor = new ScheduledThreadPoolExecutor(1, factory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

}
