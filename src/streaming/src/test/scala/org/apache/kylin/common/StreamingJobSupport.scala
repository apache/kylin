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
package org.apache.kylin.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingQueryWrapper}
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

trait StreamingJobSupport extends Eventually {
  def waitQueryReady(): StreamExecution = {
    eventually(timeout(200.second), interval(200.milliseconds)) {
      assert(SparkSession.getActiveSession.isDefined)
    }

    eventually(timeout(200.second), interval(200.milliseconds)) {
      val buildSession = SparkSession.getActiveSession.get
      val query = buildSession.sessionState.streamingQueryManager
      assert(query.active.nonEmpty)
    }

    eventually(timeout(200.second), interval(200.milliseconds)) {
      val buildSession = SparkSession.getActiveSession.get
      val activeQuery = buildSession.sessionState.streamingQueryManager.active(0).asInstanceOf[StreamingQueryWrapper].streamingQuery
      assert(activeQuery.lastProgress.sources.nonEmpty)
    }

    val buildSession = SparkSession.getActiveSession.get
    val activeQuery = buildSession.sessionState.streamingQueryManager.active(0).asInstanceOf[StreamingQueryWrapper].streamingQuery
    activeQuery
  }

  def waitQueryStop(stream: StreamExecution): Unit = {
    stream.stop()
  }
}
