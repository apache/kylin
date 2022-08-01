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
package org.apache.spark.tracker

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.kylin.engine.spark.job.BuildLayoutWithUpdate
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

class IndexTaskScheduler(tc: IndexTaskContext) extends Logging {

  private val buildLayoutWithUpdate: BuildLayoutWithUpdate = tc.buildLayoutWithUpdate
  private var indexTaskChecker: ScheduledExecutorService = _

  def startUpdateBuildProcess(): Unit = {
    indexTaskChecker = Executors.newSingleThreadScheduledExecutor
    indexTaskChecker.scheduleAtFixedRate(() => Utils.tryLogNonFatalError {
      val indexId = buildLayoutWithUpdate.updateSingleLayout(tc.seg, tc.config, tc.project)
      tc.runningIndex -= indexId
    }, 0, 1, TimeUnit.SECONDS)
  }

  def stopUpdateBuildProcess(): Unit = {
    indexTaskChecker.shutdown()
  }
}
