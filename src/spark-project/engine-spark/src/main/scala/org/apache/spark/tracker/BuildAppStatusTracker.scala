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

import java.util.Objects
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import org.apache.kylin.engine.spark.smarter.{BuildAppStatusStore, BuildListener}
import org.apache.kylin.engine.spark.utils.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.tracker.ResourceState.ResourceState
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

class BuildAppStatusTracker(val kylinConfig: KylinConfig, val sc: SparkContext,
                            val statusStore: BuildAppStatusStore) extends BuildListener with Logging {

  private val coldStart = new AtomicBoolean(true)

  private val buildResourceLoadRateThreshold: Double = kylinConfig.buildResourceLoadRateThreshold

  private val buildResourceConsecutiveIdleStateNum: Int = kylinConfig.buildResourceConsecutiveIdleStateNum

  private var resourceChecker: ScheduledExecutorService = _

  override def startMonitorBuildResourceState(): Unit = {
    if (!kylinConfig.isAdaptiveSpanningTreeEnabled) {
      // do nothing
      return
    }
    val buildResourceStateCheckInterval = kylinConfig.buildResourceStateCheckInterval
    resourceChecker = Executors.newSingleThreadScheduledExecutor
    resourceChecker.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        val (runningTaskNum, appTaskThreshold) = SparkUtils.currentResourceLoad(sc)
        statusStore.write(runningTaskNum, appTaskThreshold)
      }
    }, 0, buildResourceStateCheckInterval, TimeUnit.SECONDS)
  }

  override def shutdown(): Unit = {
    if (Objects.isNull(resourceChecker)) {
      return
    }
    resourceChecker.shutdown()
  }

  def currentResourceState(): ResourceState = {
    val currState = if (coldStart.compareAndSet(true, false)) ResourceState.Idle
    else if (statusStore.resourceStateQueue.asScala //
      .count(state => (state._1 / state._2) < buildResourceLoadRateThreshold) //
      == buildResourceConsecutiveIdleStateNum) {
      statusStore.resourceStateQueue.poll()
      ResourceState.Idle
    } else ResourceState.Fulled
    log.info(s"App ip ${sc.applicationId} curr resource state is ${currState}")
    currState
  }
}

object ResourceState extends Enumeration {
  type ResourceState = Value
  val Idle, Fulled = Value
}
