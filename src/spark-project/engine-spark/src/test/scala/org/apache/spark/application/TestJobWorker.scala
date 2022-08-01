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

package org.apache.spark.application

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.scheduler._
import org.apache.spark.scheduler.KylinJobEventLoop
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.scalatest.BeforeAndAfter

class TestJobWorker extends SparderBaseFunSuite with BeforeAndAfter {

  test("post ResourceLack event when job failed for lack of resource") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new ResourceLackJob(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receiveResourceLack = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[ResourceLack]) {
          receiveResourceLack.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and ResourceLack
    latch.await()
    assert(receiveResourceLack.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }

  test("post JobSucceeded event when job succeeded") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new MockSucceedJob(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receiveJobSucceeded = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[JobSucceeded]) {
          receiveJobSucceeded.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and JobSucceeded
    latch.await()
    assert(receiveJobSucceeded.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }

  test("post UnknownThrowable event when unknown error occurred") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new UnknownThrowableJob(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receiveUnknownThrowable = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[UnknownThrowable]) {
          receiveUnknownThrowable.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and UnknownThrowable
    latch.await()
    assert(receiveUnknownThrowable.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }
}

class UnknownThrowableJob extends SparkApplication {
  override def execute(args: Array[String]): Unit = {
    throw new NoRetryException()
  }

  override protected def doExecute(): Unit = {}
}

class ResourceLackJob extends SparkApplication {

  override def execute(args: Array[String]): Unit = {
    throw new Exception()
  }

  override protected def doExecute(): Unit = {}
}

class MockSucceedJob extends SparkApplication {
  override def execute(args: Array[String]): Unit = {}

  override protected def doExecute(): Unit = {}
}
