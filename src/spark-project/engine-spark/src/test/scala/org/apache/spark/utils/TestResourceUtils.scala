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

package org.apache.spark.utils

import org.apache.kylin.cluster.{AvailableResource, ResourceInfo, YarnClusterManager}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.utils.SparkConfHelper._
import org.apache.spark.SparkConf
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.mockito.Mockito
import org.scalatest.BeforeAndAfterEach

class TestResourceUtils extends SparderBaseFunSuite with BeforeAndAfterEach {
  private val fetcher: YarnClusterManager = Mockito.mock(classOf[YarnClusterManager])

  private val config: KylinConfig = Mockito.mock(classOf[KylinConfig])
  Mockito.when(config.getMaxAllocationResourceProportion).thenReturn(0.9)
  KylinBuildEnv.getOrCreate(config)
  Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(Integer.MAX_VALUE, Integer.MAX_VALUE))

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }


  override protected def beforeEach(): Unit = {
    super.beforeEach()
    KylinBuildEnv.getOrCreate(config)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    KylinBuildEnv.clean()
  }

  // test case: available(10, 10)  executor(20, 10) driver(1, 1)
  test("checkResource return false when available memory does not meet acquirement") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "2MB")
    conf.set(EXECUTOR_OVERHEAD, "2MB")
    conf.set(EXECUTOR_CORES, "2")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    assert(!ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: available(10, 10)  executor(10, 20) driver(2, 1)
  test("checkResource return false when available vCores does not meet acquirement") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "1MB")
    conf.set(EXECUTOR_OVERHEAD, "1MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "1MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    assert(!ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: available(12, 11)  executor(20, 20) driver(2, 1)
  test("checkResource return true when available resource is sufficient") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "2MB")
    conf.set(EXECUTOR_OVERHEAD, "2MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "1MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(12, 11), ResourceInfo(100, 100)))
    assert(ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: available(12, 11)  executor(20, 20) driver(2, 1) and instances is 1
  test("checkResource return false when only 1 instance and available resource is sufficient for half") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "10MB")
    conf.set(EXECUTOR_OVERHEAD, "10MB")
    conf.set(EXECUTOR_CORES, "20")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "1MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(12, 11), ResourceInfo(100, 100)))
    assert(!ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: available(22, 21)  executor(20, 20) driver(2, 1) and instances is 1
  test("checkResource return true when instance is 1 and available resource is sufficient") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "10MB")
    conf.set(EXECUTOR_OVERHEAD, "10MB")
    conf.set(EXECUTOR_CORES, "20")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "1MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(22, 21), ResourceInfo(100, 100)))
    assert(ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: max_capacity(120, 100) max(100, 100)  executor(100, 100) driver(1, 1)
  test("checkResource throw Exception total resource is not sufficient") {
    val config = Mockito.mock(classOf[KylinConfig])
    KylinBuildEnv.getOrCreate(config)
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "50MB")
    conf.set(EXECUTOR_OVERHEAD, "50MB")
    conf.set(EXECUTOR_CORES, "100")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(120, 100))
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    try {
      ResourceUtils.checkResource(conf, fetcher)
    } catch {
      case e: Exception => assert(e.getMessage == "Total queue resource does not meet requirement")
    }
  }

  // test case: max_capacity(1024, 5) executor(2048, 5)
  test("test KE-24591 with default kylin.engine.resource-request-over-limit-proportion") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "1024MB")
    conf.set(EXECUTOR_OVERHEAD, "1024MB")
    conf.set(EXECUTOR_CORES, "5")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(1024, 5))
    Mockito.when(config.getSparkEngineResourceRequestOverLimitProportion).thenReturn(1.0)
    try {
      ResourceUtils.checkResource(conf, fetcher)
    } catch {
      case e: Exception => assert(e.getMessage.contains("more than the maximum allocation memory capability"))
    }
  }

  // test case: max_capacity(1280, 5) executor(2048, 5)
  test("test KE-24591 with default kylin.engine.resource-request-over-limit-proportion=2.0") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "1024MB")
    conf.set(EXECUTOR_OVERHEAD, "1024MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(1280, 5))
    Mockito.when(fetcher.fetchQueueAvailableResource("default"))
      .thenReturn(AvailableResource(ResourceInfo(2048, 5), ResourceInfo(2048, 5)))
    Mockito.when(config.getSparkEngineResourceRequestOverLimitProportion).thenReturn(2.0)
    ResourceUtils.checkResource(conf, fetcher)
    assert("576MB".equals(conf.get(EXECUTOR_MEMORY)))
    assert("576MB".equals(conf.get(EXECUTOR_OVERHEAD)))
  }

  // test case: max_capacity(2000, 5) executor(2048, 5)
  test("test KE-24591 with default kylin.engine.resource-request-over-limit-proportion=1.2") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "1024MB")
    conf.set(EXECUTOR_OVERHEAD, "1024MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(2000, 5))
    Mockito.when(fetcher.fetchQueueAvailableResource("default"))
      .thenReturn(AvailableResource(ResourceInfo(2048, 5), ResourceInfo(2048, 5)))
    Mockito.when(config.getSparkEngineResourceRequestOverLimitProportion).thenReturn(1.2)
    ResourceUtils.checkResource(conf, fetcher)
    assert("900MB".equals(conf.get(EXECUTOR_MEMORY)))
    assert("900MB".equals(conf.get(EXECUTOR_OVERHEAD)))
  }

  // test case: max_capacity(1280, 5) executor(2048, 5)
  test("test KE-24591 with default kylin.engine.resource-request-over-limit-proportion=0") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "1024MB")
    conf.set(EXECUTOR_OVERHEAD, "1024MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(1280, 5))
    Mockito.when(fetcher.fetchQueueAvailableResource("default"))
      .thenReturn(AvailableResource(ResourceInfo(2048, 5), ResourceInfo(2048, 5)))
    Mockito.when(config.getSparkEngineResourceRequestOverLimitProportion).thenReturn(0)
    try {
      ResourceUtils.checkResource(conf, fetcher)
    } catch {
      case e: Exception => assert(e.getMessage.contains("more than the maximum allocation memory capability"))
    }
  }
}
