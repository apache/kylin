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

package org.apache.spark.autoheal

import java.util

import org.apache.kylin.shaded.com.google.common.collect.Maps
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.scheduler.{JobFailed, ResourceLack, RunJob}
import org.apache.kylin.engine.spark.utils.SparkConfHelper._
import org.apache.spark.SparkConf
import org.apache.spark.application.RetryInfo
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.KylinJobEventLoop
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils


object ExceptionTerminator extends Logging {

  def resolveException(rl: ResourceLack, eventLoop: KylinJobEventLoop): Unit = {
    val env = KylinBuildEnv.get()
    val result = rl.throwable match {
      case _: OutOfMemoryError =>
        resolveOutOfMemoryError(env, rl.throwable)
      case throwable: ClassNotFoundException =>
        Failed(throwable.getMessage, throwable)
      case _ =>
        incMemory(env)
    }
    result match {
      case Success(conf) =>
        KylinBuildEnv.get().buildJobInfos.recordJobRetryInfos(RetryInfo(conf, rl.throwable))
        eventLoop.post(RunJob())
      case Failed(message, throwable) => eventLoop.post(JobFailed(message, throwable))
    }
  }

  private def resolveOutOfMemoryError(env: KylinBuildEnv, throwable: Throwable): ResolverResult = {
    if (throwable.getMessage.contains(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key)) {
      logInfo("Resolve out of memory error with broadcast.")
      overrideSparkConf(env.sparkConf, SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      val overrideConf = Maps.newHashMap[String, String]()
      overrideConf.put(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      Success(overrideConf)
    } else {
      incMemory(env)
    }
  }

  private def incMemory(env: KylinBuildEnv): ResolverResult = {
    val conf = env.sparkConf
    val gradient = env.kylinConfig.getSparkEngineRetryMemoryGradient
    val prevMemory = Utils.byteStringAsMb(conf.get(EXECUTOR_MEMORY))
    val retryMemory = Math.ceil(prevMemory * gradient).toInt
    val overheadGradient = env.kylinConfig.getSparkEngineRetryOverheadMemoryGradient
    val retryOverhead = Math.ceil(retryMemory * overheadGradient).toInt

    val proportion = KylinBuildEnv.get().kylinConfig.getMaxAllocationResourceProportion
    val maxResourceMemory = (env.clusterInfoFetcher.fetchMaximumResourceAllocation.memory * proportion).toInt
    val overheadMem = Utils.byteStringAsMb(conf.get(EXECUTOR_OVERHEAD))
    val maxMemory = maxResourceMemory - overheadMem
    val maxOverheadMem = (maxMemory * overheadGradient).toInt
    if (prevMemory == maxMemory) {
      val retryCore = conf.get(EXECUTOR_CORES).toInt - 1
      if (retryCore > 0) {
        conf.set(EXECUTOR_CORES, retryCore.toString)
        logInfo(s"Reset $EXECUTOR_CORES=$retryCore when retry.")
        val overrideConf = Maps.newHashMap[String, String]()
        overrideConf.put(EXECUTOR_CORES, retryCore.toString)
        Success(overrideConf)
      } else {
        Failed(s"Retry configuration is invalid." +
          s" $EXECUTOR_CORES=$retryCore, $EXECUTOR_MEMORY=$prevMemory.", new RuntimeException)
      }
    } else if (retryMemory > maxMemory) {
      conf.set(EXECUTOR_MEMORY, maxMemory + "MB")
      conf.set(EXECUTOR_OVERHEAD, maxOverheadMem + "MB")
      logInfo(s"Reset $EXECUTOR_MEMORY=${conf.get(EXECUTOR_MEMORY)} when retry.")
      logInfo(s"Reset $EXECUTOR_OVERHEAD=${conf.get(EXECUTOR_OVERHEAD)} when retry.")
      val overrideConf = Maps.newHashMap[String, String]()
      overrideConf.put(EXECUTOR_MEMORY, maxMemory + "MB")
      overrideConf.put(EXECUTOR_OVERHEAD, maxOverheadMem + "MB")
      Success(overrideConf)
    } else {
      conf.set(EXECUTOR_MEMORY, retryMemory + "MB")
      conf.set(EXECUTOR_OVERHEAD, retryOverhead + "MB")
      logInfo(s"Reset $EXECUTOR_MEMORY=${conf.get(EXECUTOR_MEMORY)} when retry.")
      logInfo(s"Reset $EXECUTOR_OVERHEAD=${conf.get(EXECUTOR_OVERHEAD)} when retry.")
      val overrideConf = Maps.newHashMap[String, String]()
      overrideConf.put(EXECUTOR_MEMORY, retryMemory + "MB")
      overrideConf.put(EXECUTOR_OVERHEAD, retryOverhead + "MB")
      Success(overrideConf)
    }
  }

  def overrideSparkConf(sparkConf: SparkConf, key: String, value: String): Unit = {
    logInfo(s"Override spark conf $key to $value")
    sparkConf.set(key, value)
  }
}

sealed trait ResolverResult {}

case class Success(conf: util.HashMap[String, String]) extends ResolverResult

case class Failed(message: String, throwable: Throwable) extends ResolverResult