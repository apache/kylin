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

import org.apache.hadoop.security.AccessControlException

import java.util.concurrent.{CountDownLatch, TimeUnit}
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

  test("post ResourceLack event when job failed for non-AccessControlException") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new ResourceLackJobWithNonAccessControlException(), Array.empty, eventLoop)
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
    latch.await(30, TimeUnit.SECONDS)
    assert(receiveResourceLack.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }

  test("post ResourceLack event when job failed with runtime exception for lack of resource") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new ResourceLackJobWithRuntimeException(), Array.empty, eventLoop)
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
    latch.await(30, TimeUnit.SECONDS)
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

  test("post Permission denied event when PermissionDenied occurred with handle Exception function") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new InterceptPermissionDeniedJob(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receivePermissionDenied = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[UnknownThrowable]) {
          receivePermissionDenied.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and PermissionDenied
    latch.await(30, TimeUnit.SECONDS)
    assert(receivePermissionDenied.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }

  test("post Permission denied event when PermissionDenied occurred with interceptAccessControlException function") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new HandlePermissionDeniedJobWithAccessControlException(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receivePermissionDenied = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[UnknownThrowable]) {
          receivePermissionDenied.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and PermissionDenied
    latch.await(30, TimeUnit.SECONDS)
    assert(receivePermissionDenied.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }

  test("post Permission denied event when PermissionDenied occurred with RuntimeException wraped") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new HandlePermissionDeniedJobWithRuntimeExceptionWraped(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receivePermissionDenied = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[UnknownThrowable]) {
          receivePermissionDenied.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and PermissionDenied
    latch.await(30, TimeUnit.SECONDS)
    assert(receivePermissionDenied.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }

  test("post ResourceLack event when job failed for lack of resource with RuntimeException wraped") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new HandleResourceLackJobWithRuntimeExceptionWraped(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receivePermissionDenied = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[ResourceLack]) {
          receivePermissionDenied.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and PermissionDenied
    latch.await(30, TimeUnit.SECONDS)
    assert(receivePermissionDenied.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }

  test("post Permission denied event when RuntimeException occurred") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new PermissionDeniedJobWithRuntimeExceptionWarped(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receivePermissionDenied = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[UnknownThrowable]) {
          receivePermissionDenied.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and PermissionDenied
    latch.await(30, TimeUnit.SECONDS)
    assert(receivePermissionDenied.get())
    eventLoop.unregisterListener(listener)
    worker.stop()
    eventLoop.stop()
  }
  test("post Permission denied event when AccessControlException occurred") {
    val eventLoop = new KylinJobEventLoop
    eventLoop.start()
    val worker = new JobWorker(new PermissionDeniedJobWithNoRetryException(), Array.empty, eventLoop)
    val latch = new CountDownLatch(2)
    val receivePermissionDenied = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[UnknownThrowable]) {
          receivePermissionDenied.getAndSet(true)
        }
        latch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(RunJob())
    // receive RunJob and PermissionDenied
    latch.await(30, TimeUnit.SECONDS)
    assert(receivePermissionDenied.get())
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

class InterceptPermissionDeniedJob extends SparkApplication {
  override def execute(args: Array[String]): Unit = {
    try {
      throw new AccessControlException()
    } catch {
      case e : AccessControlException =>
        interceptAccessControlException(e)
    }
  }
  override protected def doExecute(): Unit = {}
}

class HandlePermissionDeniedJobWithAccessControlException extends SparkApplication {
  override def execute(args: Array[String]): Unit = {
    try {
      throw new AccessControlException()
    } catch {
      case e: Exception => handleException(e)
    }
  }
  override protected def doExecute(): Unit = {}
}

class ResourceLackJobWithNonAccessControlException extends SparkApplication {
  override def execute(args: Array[String]): Unit = {
    try {
      throw new Exception
    } catch {
      case e: Exception => handleException(e)
    }
  }
  override protected def doExecute(): Unit = {}
}

class HandlePermissionDeniedJobWithRuntimeExceptionWraped extends SparkApplication {
  override def execute(args: Array[String]): Unit = {
    try {
      throw new AccessControlException()
    } catch {
      case e: Exception => handleException(new RuntimeException("PermissionDenied", e))
    }
  }
  override protected def doExecute(): Unit = {}
}

class HandleResourceLackJobWithRuntimeExceptionWraped extends SparkApplication {
  override def execute(args: Array[String]): Unit = {
    try {
      throw new Exception()
    } catch {
      case e: Exception => handleException(new RuntimeException("Resource Lack", e))
    }
  }
  override protected def doExecute(): Unit = {}
}


class PermissionDeniedJobWithRuntimeExceptionWarped extends SparkApplication {
  override def execute(args: Array[String]): Unit = {
    try {
      throw new AccessControlException()
    } catch {
      case e : Exception => throw new RuntimeException("Error execute " + this.getClass.getName, new NoRetryException("Permission denied."))
    }
  }
  override protected def doExecute(): Unit = {}
}

class PermissionDeniedJobWithNoRetryException extends SparkApplication {
  override def execute(args: Array[String]): Unit = {
      throw new NoRetryException("Permission Denied")
  }
  override protected def doExecute(): Unit = {}
}

class ResourceLackJob extends SparkApplication {

  override def execute(args: Array[String]): Unit = {
    throw new Exception()
  }

  override protected def doExecute(): Unit = {}
}

class ResourceLackJobWithRuntimeException extends SparkApplication {

  override def execute(args: Array[String]): Unit = {
    try {
      throw new Exception()
    } catch {
      case e: Exception => throw new RuntimeException("Error execute " + this.getClass.getName, e)
    }
  }

  override protected def doExecute(): Unit = {}
}

class MockSucceedJob extends SparkApplication {
  override def execute(args: Array[String]): Unit = {}

  override protected def doExecute(): Unit = {}
}
