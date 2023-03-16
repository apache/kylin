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

package org.apache.kylin.engine.spark.builder

import org.apache.kylin.guava30.shaded.common.collect.Sets
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.scheduler.ClusterMonitor
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}

class TestClusterMonitor extends AnyFunSuite {
  def getTestConfig: KylinConfig = {
    KylinConfig.createKylinConfig(new String())
  }

  test("test monitorSparkMaster: cloud and PROD and default max times") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "PROD")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
  }

  test("test monitorSparkMaster: cloud and PROD and max times: -1") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "PROD")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "-1")
    config.setProperty("kylin.engine.cluster-manager-heal-check-interval-second", "1")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
  }

  test("test monitorSparkMaster: cloud and PROD and max times: 0") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "PROD")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "0")
    config.setProperty("kylin.engine.cluster-manager-heal-check-interval-second", "1")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 1)
    assert(atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "-1")
  }

  test("test monitorSparkMaster: cloud and PROD and max times: 10") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "PROD")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "10")
    config.setProperty("kylin.engine.cluster-manager-heal-check-interval-second", "1")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(Sets.newHashSet(2L, 3L, 4L).contains(result.get()))
    assert(atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "-1")
  }

  test("test monitorSparkMaster: on-premises and PROD") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "PROD")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env", "UT")
  }


  test("test monitorSparkMaster: on-premises and UT") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
  }

  test("test monitorSparkMaster: cloud and UT") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "UT")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
  }
}

class MockClusterMonitor extends ClusterMonitor {
  override def monitor(atomicBuildEnv: AtomicReference[KylinBuildEnv], atomicSparkSession: AtomicReference[SparkSession],
                       disconnectTimes: AtomicLong, atomicUnreachableSparkMaster: AtomicBoolean): Unit = {
    val config = atomicBuildEnv.get().kylinConfig
    if (disconnectTimes.get() <= config.getClusterManagerHealthCheckMaxTimes) {
      disconnectTimes.addAndGet(config.getClusterManagerHealCheckIntervalSecond)
      atomicUnreachableSparkMaster.set(true)
    }
  }
}
