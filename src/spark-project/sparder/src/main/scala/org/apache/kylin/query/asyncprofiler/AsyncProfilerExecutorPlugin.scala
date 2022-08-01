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

package org.apache.kylin.query.asyncprofiler

import java.util
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

class AsyncProfilerExecutorPlugin extends ExecutorPlugin with Logging {

  private val checkingInterval: Long = 1000
  private var ctx: PluginContext = _
  private var dumped = false

  private val scheduledExecutorService = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("profiler-%d").build())

  override def init(_ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    ctx = _ctx
    val profile = new Runnable {
      override def run(): Unit = checkAndProfile()
    }
    log.debug(s"AsyncProfiler status: ${AsyncProfilerTool.status()}")
    scheduledExecutorService.scheduleWithFixedDelay(
      profile, 0, checkingInterval, TimeUnit.MILLISECONDS)
  }

  def checkAndProfile(): Unit = {
    import Message._
    try {
      val reply = ask(createExecutorMessage(NEXT_COMMAND, ctx.executorID()))
      val (command, _, param) = Message.processMessage(reply)
      command match {
        case START if !AsyncProfilerTool.running() =>
          dumped = false
          AsyncProfilerTool.start(param)
        case STOP if AsyncProfilerTool.running() =>
          AsyncProfilerTool.stop()
        case DUMP if !dumped => // doesn't allow a second dump for simplicity
          val result = AsyncProfilerTool.dump(param)
          AsyncProfilerTool.stop() // call stop anyway to make sure profiler is stopped
          dumped = true
          send(createExecutorMessage(RESULT, ctx.executorID(), result))
        case _ =>
      }
    } catch {
      case e: Exception =>
        logInfo("error while communication/profiling", e)
    }
  }

  private def ask(msg: String): String = {
    logTrace(s"ask: $msg")
    val result = ctx.ask(msg).toString
    logTrace(s"ask result: $result")
    result
  }

  private def send(msg: String): Unit = {
    logTrace(s"send: $msg")
    ctx.send(msg)
  }

}
