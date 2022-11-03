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

package org.apache.kylin.common.asyncprofiler

import org.slf4j.{Logger, LoggerFactory}

/**
 * this class is not thread safe
 */
object AsyncProfilerTool {

  val log: Logger = LoggerFactory.getLogger(AsyncProfilerTool.getClass)

  private val profiler = AsyncProfiler.getInstance()

  private var _running = false

  def running(): Boolean = {
    _running
  }

  def start(params: String): Unit = {
    try {
      log.debug("start profiling")
      _running = true
      profiler.execute(params)
    } catch {
      case e: Exception =>
        log.error("failed start profiling", e)
    }
  }

  def dump(params: String): String = {
    log.debug("stop and dump profiling")
    try {
      _running = false
      profiler.execute(params)
    } catch {
      case e: Exception =>
        log.error("failed dump profiling", e)
        e.toString
    }
  }

  def stop(): Unit = {
    try {
      _running = false
      profiler.stop()
    } catch {
      case e: Exception =>
        log.trace("failed stop profiling", e)
    }
  }

  def status(): String = {
    profiler.execute("status")
  }

  def execute(cmd: String): String = {
    try {
      profiler.execute(cmd)
    } catch {
      case e: Exception =>
        log.error("failed exec profiling", e)
        ""
    }
  }
}
