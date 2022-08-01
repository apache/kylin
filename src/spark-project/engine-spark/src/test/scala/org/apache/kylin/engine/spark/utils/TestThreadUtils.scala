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

package org.apache.kylin.engine.spark.utils

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.sql.common.SparderBaseFunSuite
import org.junit.Assert

class TestThreadUtils extends SparderBaseFunSuite {

  test("newDaemonScalableThreadPool") {
    val threadPool = //
      ThreadUtils.newDaemonScalableThreadPool("mock-thread", //
        10, 100, 10, TimeUnit.SECONDS)
    try {
      val total = 200
      val cdl = new CountDownLatch(total)
      var i = total
      while (i > 0) {
        threadPool.submit(new Runnable {
          override def run(): Unit = {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1))
            cdl.countDown()
          }

          i -= 1
        })
      }
      Assert.assertTrue(cdl.await(10, TimeUnit.SECONDS))
    } finally {
      threadPool.shutdownNow()
    }
  }

  test("newDaemonSingleThreadScheduledExecutor") {
    val scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("this-is-a-thread-name")
    try {
      val cdl = new CountDownLatch(1)
      @volatile var threadName = ""
      scheduler.schedule(new Runnable {
        override def run(): Unit = {
          threadName = Thread.currentThread().getName
          cdl.countDown()
        }
      }, 200, TimeUnit.MILLISECONDS)
      cdl.await(10, TimeUnit.SECONDS)
      assert(threadName === "this-is-a-thread-name")
    } finally {
      scheduler.shutdownNow()
    }
  }

}
