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

import org.apache.kylin.common.asyncprofiler.AsyncProfilerTool
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.internal.Logging

import java.util

class QueryAsyncProfilerDriverPlugin extends DriverPlugin with Logging {

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    // Sparder Driver and KE are always in one JVM, in client mode
    AsyncProfilerTool.loadAsyncProfilerLib(true)
    super.init(sc, pluginContext)
  }

  override def receive(message: Any): AnyRef = {
    import org.apache.kylin.common.asyncprofiler.Message._

    val (command, executorId, param) = processMessage(message.toString)
    command match {
      case NEXT_COMMAND =>
        AsyncProfiling.nextCommand()
      case RESULT =>
        AsyncProfiling.cacheExecutorResult(param, executorId)
        ""
      case _ => ""
    }
  }
}