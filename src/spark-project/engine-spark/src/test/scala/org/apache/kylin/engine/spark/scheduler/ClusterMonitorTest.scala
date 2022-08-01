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

import org.apache.kylin.cluster.ResourceInfo
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.spark.application.{MockClusterManager, NoRetryException}
import org.apache.spark.scheduler.KylinJobEventLoop
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}

class ClusterMonitorTest extends AnyFunSuite {
  val config = Mockito.mock(classOf[KylinConfig])
  Mockito.when(config.getMaxAllocationResourceProportion).thenReturn(1.0)
  Mockito.when(config.getSparkEngineRetryMemoryGradient).thenReturn(1.5)
  Mockito.when(config.getSparkEngineRetryOverheadMemoryGradient).thenReturn(0.2)
  Mockito.when(config.getClusterManagerClassName).thenReturn("org.apache.spark.application.MockClusterManager")
  Mockito.when(config.getClusterManagerTimeoutThreshold).thenReturn(10 * 1000)
  Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)

  test("test scheduleWithFixedDelay") {
    val cdl = new CountDownLatch(1)
    val result = new AtomicLong(0)
    val cm = new ClusterMonitor()
    @volatile var threadName = ""
    cm.scheduleAtFixedRate(() => {
      result.addAndGet(5)
      threadName = Thread.currentThread().getName
    }, 5)
    cdl.await(10, TimeUnit.SECONDS)
    cm.shutdown()
    assert(result.get() == 10)
    assert(threadName === "connect-master-guard")
  }

  test("test monitor") {
    val env = KylinBuildEnv.getOrCreate(config)
    java.lang.reflect.Proxy.getInvocationHandler(env.clusterManager).invoke(env.clusterManager,
      classOf[MockClusterManager].getMethod("setMaxAllocation", classOf[ResourceInfo]),
      Array(ResourceInfo(2400, Int.MaxValue)))

    val result = new AtomicLong(10)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val cm = new ClusterMonitor()
    cm.monitor(new AtomicReference[KylinBuildEnv](env), new AtomicReference[SparkSession](null), result, atomicUnreachableSparkMaster)
    KylinBuildEnv.clean()

    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
  }

  test("test monitor with error") {
    val env = KylinBuildEnv.getOrCreate(config)
    Mockito.doThrow(new RuntimeException("test monitor with error")).when(config).getClusterManagerClassName
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val ss = Mockito.mock(classOf[SparkSession])
    val atomicSparkSession = new AtomicReference[SparkSession](ss)
    Mockito.doNothing().when(ss).stop()
    val eventLoop = Mockito.mock(classOf[KylinJobEventLoop])
    val errorMsg = "Unable to connect to spark master to reach set timeout maximum time"
    Mockito.doNothing().when(eventLoop).post(JobFailed(errorMsg, new NoRetryException(errorMsg)))

    val result = new AtomicLong(10)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val cm = new ClusterMonitor()
    cm.monitor(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    assert(result.get() >= 10)
    assert(atomicUnreachableSparkMaster.get())

    result.set(0)
    atomicUnreachableSparkMaster.set(false)
    Mockito.when(config.getClusterManagerHealthCheckMaxTimes).thenReturn(20)
    Mockito.when(config.getClusterManagerHealCheckIntervalSecond).thenReturn(10)
    cm.monitor(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    assert(result.get() >= 1)
    assert(!atomicUnreachableSparkMaster.get())

    result.set(0)
    atomicUnreachableSparkMaster.set(false)
    Mockito.when(config.getClusterManagerHealthCheckMaxTimes).thenReturn(1)
    Mockito.when(config.getClusterManagerHealCheckIntervalSecond).thenReturn(10)
    cm.monitor(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    assert(result.get() >= 1)
    assert(atomicUnreachableSparkMaster.get())

    KylinBuildEnv.clean()
  }

  test("test monitor with error and spark session is null") {
    val env = KylinBuildEnv.getOrCreate(config)
    Mockito.doThrow(new RuntimeException("test monitor with error")).when(config).getClusterManagerClassName
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val eventLoop = Mockito.mock(classOf[KylinJobEventLoop])
    val errorMsg = "Unable to connect to spark master to reach set timeout maximum time"
    Mockito.doNothing().when(eventLoop).post(JobFailed(errorMsg, new NoRetryException(errorMsg)))

    val result = new AtomicLong(10)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val cm = new ClusterMonitor()
    cm.monitor(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    assert(result.get() >= 10)
    assert(!atomicUnreachableSparkMaster.get())

    result.set(0)
    atomicUnreachableSparkMaster.set(true)
    Mockito.when(config.getClusterManagerHealthCheckMaxTimes).thenReturn(20)
    Mockito.when(config.getClusterManagerHealCheckIntervalSecond).thenReturn(10)
    cm.monitor(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    assert(result.get() >= 1)
    assert(atomicUnreachableSparkMaster.get())

    result.set(0)
    atomicUnreachableSparkMaster.set(false)
    Mockito.when(config.getClusterManagerHealthCheckMaxTimes).thenReturn(1)
    Mockito.when(config.getClusterManagerHealCheckIntervalSecond).thenReturn(10)
    cm.monitor(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    assert(result.get() >= 1)
    assert(!atomicUnreachableSparkMaster.get())

    KylinBuildEnv.clean()
  }

  test("test sparkApplication extraDestroy") {
    var app = new SparkApplication() {
      @throws[Exception]
      override protected def doExecute(): Unit = {
      }
    }
    app.extraDestroy()
  }
}
