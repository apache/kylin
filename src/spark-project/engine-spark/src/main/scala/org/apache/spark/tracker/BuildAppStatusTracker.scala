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
import org.apache.kylin.engine.spark.smarter.{BuildAppStatusStore, BuildListener}
import org.apache.kylin.engine.spark.utils.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.tracker.ResourceState.ResourceState
import org.apache.spark.util.Utils

import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService, TimeUnit}
import java.util.{Objects, Timer, TimerTask}
import scala.collection.JavaConverters._

class BuildAppStatusTracker(val kylinConfig: KylinConfig, val sc: SparkContext,
                            val statusStore: BuildAppStatusStore) extends BuildListener with Logging {

  private val buildResourceLoadRateThreshold: Double = kylinConfig.buildResourceLoadRateThreshold

  private val buildResourceStateCheckInterval: Long = kylinConfig.buildResourceStateCheckInterval

  private val stateWindTimer = new Timer("state-window-timer", true)

  private var resourceChecker: ScheduledExecutorService = _

  override def startMonitorBuildResourceState(): Unit = {
    resourceChecker = Executors.newSingleThreadScheduledExecutor
    resourceChecker.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        val (runningTaskNum, appTaskThreshold) = SparkUtils.currentResourceLoad(sc)
        statusStore.write(runningTaskNum, appTaskThreshold)
      }
    }, 0, buildResourceStateCheckInterval, TimeUnit.SECONDS)
  }

  override def shutdown(): Unit = {
    if (Objects.nonNull(resourceChecker)) {
      resourceChecker.shutdownNow()
    }

    if (Objects.nonNull(stateWindTimer)) {
      stateWindTimer.cancel()
    }
  }

  def currentResourceState(): ResourceState = {
    val currState = getResourceState
    log.info(s"Application ${sc.applicationId} current resource state is $currState")
    currState
  }

  private def getResourceState: ResourceState = {
    val stateWind = statusStore.resourceStateQueue
    if (stateWind.remainingCapacity() > 0) {
      val cdl = new CountDownLatch(1)
      stateWindTimer.scheduleAtFixedRate(new TimerTask {
        override def run(): Unit = {
          if (stateWind.remainingCapacity() > 0) {
            logInfo("Resource state window's remaining capacity still be greater than 0.")
          } else {
            this.cancel()
            cdl.countDown()
          }
        }
      }, 0, TimeUnit.SECONDS.toMillis(Math.max(1, buildResourceStateCheckInterval >> 1)))
      cdl.await(buildResourceStateCheckInterval, TimeUnit.SECONDS)
    }

    if (stateWind.asScala.forall(state => (state._1.toDouble / state._2) < buildResourceLoadRateThreshold)) {
      // shift right the window
      stateWind.poll(1, TimeUnit.SECONDS)
      return ResourceState.Idle
    }

    ResourceState.Fulled
  }
}

object ResourceState extends Enumeration {
  type ResourceState = Value
  val Idle, Fulled = Value
}
