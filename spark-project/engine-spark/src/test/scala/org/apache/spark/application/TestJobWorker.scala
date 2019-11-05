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

package org.apache.spark.application

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import io.kyligence.kap.engine.spark.application.SparkApplication
import io.kyligence.kap.engine.spark.scheduler._
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
