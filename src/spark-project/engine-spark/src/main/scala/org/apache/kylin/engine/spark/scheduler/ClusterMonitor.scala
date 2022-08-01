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

import org.apache.kylin.common.KapConfig
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.utils.ThreadUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}


class ClusterMonitor extends Logging {
  private lazy val scheduler = //
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("connect-master-guard")
  private val JOB_STEP_PREFIX = "job_step_"

  def scheduleAtFixedRate(func: () => Unit, period: Long): Unit = {
    scheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = func.apply()
    }, period, period, TimeUnit.SECONDS)
  }

  def monitorSparkMaster(atomicBuildEnv: AtomicReference[KylinBuildEnv], atomicSparkSession: AtomicReference[SparkSession],
                         disconnectTimes: AtomicLong, atomicUnreachableSparkMaster: AtomicBoolean): Unit = {
    val config = atomicBuildEnv.get().kylinConfig
    if (KapConfig.wrap(config).isCloud && !config.isUTEnv) {
      val disconnectMaxTimes = config.getClusterManagerHealthCheckMaxTimes
      if (disconnectMaxTimes >= 0) {
        val connectPeriod = config.getClusterManagerHealCheckIntervalSecond
        logDebug(s"ClusterMonitor thread start with max times is $disconnectMaxTimes period is $connectPeriod")
        scheduleAtFixedRate(() => {
          monitor(atomicBuildEnv, atomicSparkSession, disconnectTimes, atomicUnreachableSparkMaster)
        }, connectPeriod)
      }
    }
  }

  def monitor(atomicBuildEnv: AtomicReference[KylinBuildEnv], atomicSparkSession: AtomicReference[SparkSession],
              disconnectTimes: AtomicLong, atomicUnreachableSparkMaster: AtomicBoolean): Unit = {
    logDebug("monitor start")
    val config = atomicBuildEnv.get().kylinConfig
    val disconnectMaxTimes = config.getClusterManagerHealthCheckMaxTimes
    try {
      val clusterManager = atomicBuildEnv.get.clusterManager
      clusterManager.applicationExisted(JOB_STEP_PREFIX + atomicBuildEnv.get().buildJobInfos.getJobStepId)
      disconnectTimes.set(0)
      atomicUnreachableSparkMaster.set(false)
      logDebug("monitor stop")
    } catch {
      case e: Exception =>
        logError(s"monitor error with : ${e.getMessage}")
        if (disconnectTimes.incrementAndGet() >= disconnectMaxTimes && atomicSparkSession.get() != null) {
          logDebug(s"Job will stop: Unable connect spark master to reach timeout maximum time")
          atomicUnreachableSparkMaster.set(true)
          atomicSparkSession.get().stop()
        }
    }
  }

  def shutdown(): Unit = {
    scheduler.shutdownNow()
  }
}
