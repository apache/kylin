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

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.smarter.BuildAppStatusStore
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import java.util.Objects
import scala.util.control.NonFatal

class BuildContext(sparkContext: SparkContext, kylinConfig: KylinConfig) extends Logging {

  /* ------------------------------------------------------------------------------------- *
 | Private variables. These variables keep the internal state of the context, and are    |
 | not accessible by the outside world.                                                  |
 * ------------------------------------------------------------------------------------- */

  private var _appStatusTracker: BuildAppStatusTracker = _
  private var _appStatusStore: BuildAppStatusStore = _

  /* ------------------------------------------------------------------------------------- *
 | Accessors and public fields. These provide access to the internal state of the        |
 | context.                                                                              |
 * ------------------------------------------------------------------------------------- */

  def appStatusTracker: BuildAppStatusTracker = _appStatusTracker

  def appStatusStore: BuildAppStatusStore = _appStatusStore

  try {
    _appStatusStore = new BuildAppStatusStore(kylinConfig, sparkContext)
    _appStatusTracker = new BuildAppStatusTracker(kylinConfig, sparkContext, _appStatusStore)
  } catch {
    case NonFatal(e) =>
      logError("Error initializing BuildContext.", e)
  }

  def isAvailable: Boolean = {
    appStatusTracker.currentResourceState() == ResourceState.Idle
  }

  def stop(): Unit = {
    if (Objects.nonNull(_appStatusTracker)) {
      Utils.tryLogNonFatalError {
        _appStatusTracker.shutdown()
      }
    }
    logInfo("Stop BuildContext succeed.")
  }
}
