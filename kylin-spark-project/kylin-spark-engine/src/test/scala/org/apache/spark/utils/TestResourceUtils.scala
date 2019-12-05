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

import com.google.common.collect.Maps
import io.kyligence.kap.cluster.{AvailableResource, ResourceInfo, YarnInfoFetcher}
import io.kyligence.kap.engine.spark.utils.SparkConfHelper._
import org.apache.spark.SparkConf
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.sql.hive.utils.DetectItem
import org.mockito.Mockito

class TestResourceUtils extends SparderBaseFunSuite {
  private val fetcher: YarnInfoFetcher = Mockito.mock(classOf[YarnInfoFetcher])

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

  // test case: max(100, 100)  executor(100, 100) driver(1, 1)
  test("checkResource throw Exception total resource is not sufficient") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "50MB")
    conf.set(EXECUTOR_OVERHEAD, "50MB")
    conf.set(EXECUTOR_CORES, "100")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    try {
      ResourceUtils.checkResource(conf, fetcher)
    } catch {
      case e: Exception => assert(e.getMessage == "Total queue resource does not meet requirement")
    }
  }


  test("test caculateRequiredCores") {
    val detectedItems: java.util.Map[String, String] = Maps.newLinkedHashMap()
    detectedItems.put(DetectItem.ESTIMATED_LINE_COUNT, "10000")
    detectedItems.put(DetectItem.ESTIMATED_SIZE, "23120216")
    assert(ResourceUtils.caculateRequiredCores("300m", detectedItems, 20000000) == "147")
    assert(ResourceUtils.caculateRequiredCores("300m", detectedItems, 136055) == "1")
    detectedItems.put(DetectItem.ESTIMATED_LINE_COUNT, "0")
    assert(ResourceUtils.caculateRequiredCores("300m", detectedItems, 136055) == "1")

  }

}
