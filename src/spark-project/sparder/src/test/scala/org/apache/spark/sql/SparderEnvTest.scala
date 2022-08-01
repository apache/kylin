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

package org.apache.spark.sql

import org.apache.kylin.common.exception.KylinTimeoutException

import java.util.concurrent.CountDownLatch
import org.apache.spark.SparkConf
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}

// scalastyle:off
class SparderEnvTest extends SparderBaseFunSuite with LocalMetadata {

  test("getExecutorNum when dynamicAllocation enabled") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.executor.instances", "1")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "5")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "0")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.shuffle.service.port", "7337")

    assert(5 == SparderEnv.getExecutorNum(sparkConf))
  }

  test("getExecutorNum when dynamicAllocation disabled") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.executor.instances", "1")
    sparkConf.set("spark.dynamicAllocation.enabled", "false")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "5")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "0")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.shuffle.service.port", "7337")

    assert(1 == SparderEnv.getExecutorNum(sparkConf))
  }

  test("sparder env concurrent init") {
    var initCount = 0
    def init(): Unit = {
      SparderEnv.initSpark(() => {
        Thread.sleep(1000)
        initCount += 1
      })
    }

    var th1Elapsed = System.currentTimeMillis();
    var th2Elapsed = System.currentTimeMillis();
    var th3Elapsed = System.currentTimeMillis();
    val th1 = new Thread(() => {
      init()
      th1Elapsed = System.currentTimeMillis() - th1Elapsed
    })
    val th2 = new Thread(() => {
      init()
      th2Elapsed = System.currentTimeMillis() - th1Elapsed
    })
    val th3 = new Thread(() => {
      init()
      th3Elapsed = System.currentTimeMillis() - th1Elapsed
    })

    Seq(th1, th2, th3).foreach(th => th.start())
    Seq(th1, th2, th3).foreach(th => th.join())
    assert(initCount == 1)
    assert(th1Elapsed + th2Elapsed + th3Elapsed >= 3000);
  }

  test("sparder env concurrent init - interrupt") {
    var initCount = 0
    val latch = new CountDownLatch(1)
    def init(): Unit = {
      SparderEnv.initSpark(() => {
        latch.countDown()
        Thread.sleep(1000)
        initCount += 1
      })
    }

    var th1Interrupted = false
    val th1 = new Thread(() => try {
      init()
    } catch {
      case _: KylinTimeoutException =>
        logInfo("interrupted corrected")
        th1Interrupted = true
      case _ =>
        fail("not interrupted corrected")
    })
    th1.start()
    latch.await()
    th1.interrupt()

    val th2 = new Thread(() => init())
    val th3 = new Thread(() => init())
    th2.start()
    th3.start()

    th2.join()
    th3.join()
    assert(initCount == 1)
    assert(th1Interrupted)
  }
}

