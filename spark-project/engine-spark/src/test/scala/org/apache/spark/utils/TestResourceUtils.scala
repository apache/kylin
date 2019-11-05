/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
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
