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

import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.apache.kylin.cluster.{AvailableResource, IClusterManager, ResourceInfo}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.scheduler._
import org.apache.kylin.engine.spark.utils.SparkConfHelper._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.scheduler.KylinJobEventLoop
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils
import org.mockito.Mockito
import org.scalatest.BeforeAndAfterEach

class TestJobMonitor extends SparderBaseFunSuite with BeforeAndAfterEach {
  private val config = Mockito.mock(classOf[KylinConfig])
  private val gradient = 1.5
  private val overheadGradient = 0.2
  private val proportion = 1.0
  Mockito.when(config.getMaxAllocationResourceProportion).thenReturn(proportion)
  Mockito.when(config.getSparkEngineRetryMemoryGradient).thenReturn(gradient)
  Mockito.when(config.getSparkEngineRetryOverheadMemoryGradient).thenReturn(overheadGradient)
  Mockito.when(config.getClusterManagerClassName).thenReturn("org.apache.spark.application.MockClusterManager")
  Mockito.when(config.getClusterManagerTimeoutThreshold).thenReturn(10 * 1000)

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    KylinBuildEnv.clean()
  }

  def withEventLoop(body: KylinJobEventLoop => Unit): Unit = {
    val loop = new KylinJobEventLoop
    loop.start()
    try body(loop)
    finally loop.stop()
  }


  test("post ExceedMaxRetry event when current retry times greater than Max") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(0)
      KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val receiveExceedMaxRetry = new AtomicBoolean(false)
      val countDownLatch = new CountDownLatch(3)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          if (event.isInstanceOf[ExceedMaxRetry]) {
            receiveExceedMaxRetry.getAndSet(true)
          }
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new Exception()))
      // receive ResourceLack, ExceedMaxRetry and JobFailed
      countDownLatch.await()
      assert(receiveExceedMaxRetry.get())
      eventLoop.unregisterListener(listener)
    }
  }

  test("rest spark.executor.cores when receive ResourceLack event and preMemory eq (maxAllocation - overhead)") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
      val env = KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val memory = "2000MB"
      val overhead = "400MB"
      val cores = "2"
      val maxAllocation = 2400
      java.lang.reflect.Proxy.getInvocationHandler(env.clusterManager).invoke(env.clusterManager,
        classOf[MockClusterManager].getMethod("setMaxAllocation", classOf[ResourceInfo]),
        Array(ResourceInfo(maxAllocation, Int.MaxValue)))
      env.sparkConf.set(EXECUTOR_MEMORY, memory)
      env.sparkConf.set(EXECUTOR_OVERHEAD, overhead)
      env.sparkConf.set(EXECUTOR_CORES, cores)
      val countDownLatch = new CountDownLatch(2)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new Exception()))
      // receive ResourceLack and RunJob
      countDownLatch.await()
      assert(env.sparkConf.get(EXECUTOR_MEMORY) == memory)
      assert(env.sparkConf.get(EXECUTOR_CORES) == (cores.toInt - 1).toString)
      assert(System.getProperty("kylin.spark-conf.auto-prior") == "false")
      eventLoop.unregisterListener(listener)
    }
  }

  test("post JobFailed when receive ResourceLack event and preMemory eq (maxAllocation - overhead) and retryCores eq 0") {
    withEventLoop { eventLoop =>
      val env = KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val memory = "2000MB"
      val overhead = "400MB"
      val cores = "1"
      val maxAllocation = 2400
      java.lang.reflect.Proxy.getInvocationHandler(env.clusterManager).invoke(env.clusterManager,
        classOf[MockClusterManager].getMethod("setMaxAllocation", classOf[ResourceInfo]),
        Array(ResourceInfo(maxAllocation, Int.MaxValue)))
      env.sparkConf.set(EXECUTOR_MEMORY, memory)
      env.sparkConf.set(EXECUTOR_OVERHEAD, overhead)
      env.sparkConf.set(EXECUTOR_CORES, cores)
      val receiveJobFailed = new AtomicBoolean(false)
      val countDownLatch = new CountDownLatch(2)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          if (event.isInstanceOf[JobFailed]) {
            receiveJobFailed.getAndSet(true)
          }
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new Exception()))
      // receive ResourceLack and JobFailed
      countDownLatch.await()
      assert(receiveJobFailed.get())
      eventLoop.unregisterListener(listener)
    }
  }

  test("rest spark.executor.memory to maxAllocation/(overheadGradient + 1) when receive ResourceLack event " +
    "and retryMemory gt maxAllocation/(overheadGradient + 1) and prevMemory le maxAllocation/(overheadGradient + 1)") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
      val env = KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val memory = "2000MB"
      val overhead = "400MB"
      val maxAllocation = 2500
      java.lang.reflect.Proxy.getInvocationHandler(env.clusterManager).invoke(env.clusterManager,
        classOf[MockClusterManager].getMethod("setMaxAllocation", classOf[ResourceInfo]),
        Array(ResourceInfo(maxAllocation, Int.MaxValue)))
      env.sparkConf.set(EXECUTOR_MEMORY, memory)
      env.sparkConf.set(EXECUTOR_OVERHEAD, overhead)
      val countDownLatch = new CountDownLatch(2)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new Exception()))
      // receive ResourceLack and RunJob
      countDownLatch.await()
      assert(env.sparkConf.get(EXECUTOR_MEMORY) == (maxAllocation / (overheadGradient + 1)).toInt + "MB")
      assert(env.sparkConf.get(EXECUTOR_OVERHEAD) == (maxAllocation / (overheadGradient + 1) * overheadGradient).toInt + "MB")
      assert(System.getProperty("kylin.spark-conf.auto-prior") == "false")
      eventLoop.unregisterListener(listener)
    }
  }

  test("rest spark.executor.memory failed when receive ResourceLack event " +
    "and retryMemory gt maxAllocation/(overheadGradient + 1) and prevMemory gt maxAllocation/(overheadGradient + 1)") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
      val env = KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val memory = "3000MB"
      val overhead = "400MB"
      val maxAllocation = 2500
      java.lang.reflect.Proxy.getInvocationHandler(env.clusterManager).invoke(env.clusterManager,
        classOf[MockClusterManager].getMethod("setMaxAllocation", classOf[ResourceInfo]),
        Array(ResourceInfo(maxAllocation, Int.MaxValue)))
      env.sparkConf.set(EXECUTOR_MEMORY, memory)
      env.sparkConf.set(EXECUTOR_OVERHEAD, overhead)
      val receiveJobFailed = new AtomicBoolean(false)
      val countDownLatch = new CountDownLatch(2)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          if (event.isInstanceOf[JobFailed]) {
            receiveJobFailed.getAndSet(true)
          }
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new Exception()))
      // receive ResourceLack and RunJob
      countDownLatch.await()
      assert(receiveJobFailed.get())
      assert(System.getProperty("kylin.spark-conf.auto-prior") == "false")
      eventLoop.unregisterListener(listener)
    }
  }

  test("rest spark.executor.memory to retryMemory when receive ResourceLack event and retryMemory lte (maxAllocation - overhead) " +
    "and prevMemory le (maxAllocation - overhead)") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
      val env = KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val memory = "2000MB"
      val overhead = "400MB"
      val maxAllocation = 4000
      java.lang.reflect.Proxy.getInvocationHandler(env.clusterManager).invoke(env.clusterManager,
        classOf[MockClusterManager].getMethod("setMaxAllocation", classOf[ResourceInfo]),
        Array(ResourceInfo(maxAllocation, Int.MaxValue)))
      env.sparkConf.set(EXECUTOR_MEMORY, memory)
      env.sparkConf.set(EXECUTOR_OVERHEAD, overhead)
      val countDownLatch = new CountDownLatch(2)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new Exception()))
      // receive ResourceLack and RunJob
      countDownLatch.await()
      assert(env.sparkConf.get(EXECUTOR_MEMORY) == Math.ceil(Utils.byteStringAsMb(memory) * gradient).toInt + "MB")
      assert(System.getProperty("kylin.spark-conf.auto-prior") == "false")
      eventLoop.unregisterListener(listener)
    }
  }

  test("retry with original configuration when receive ResourceLack event including specific exception") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
      Mockito.when(config.getJobResourceLackIgnoreExceptionClasses).thenReturn(Array("com.amazonaws.services.s3.model.AmazonS3Exception"))
      val env = KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val memory = "2000MB"
      val cores = "2"
      val maxAllocation = 2400
      java.lang.reflect.Proxy.getInvocationHandler(env.clusterManager).invoke(env.clusterManager,
        classOf[MockClusterManager].getMethod("setMaxAllocation", classOf[ResourceInfo]),
        Array(ResourceInfo(maxAllocation, Int.MaxValue)))
      env.sparkConf.set(EXECUTOR_MEMORY, memory)
      env.sparkConf.set(EXECUTOR_CORES, cores)
      val countDownLatch = new CountDownLatch(2)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new AmazonS3Exception("test")))
      // receive ResourceLack and RunJob
      countDownLatch.await()
      assert(env.sparkConf.get(EXECUTOR_MEMORY) == memory)
      assert(env.sparkConf.get(EXECUTOR_CORES) == cores)
      assert(System.getProperty("kylin.spark-conf.auto-prior") == "false")
      eventLoop.unregisterListener(listener)
    }
  }

  test("post JobFailed event when receive UnknownThrowable event") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
      KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val countDownLatch = new CountDownLatch(2)
      val receiveJobFailed = new AtomicBoolean(false)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          if (event.isInstanceOf[JobFailed]) {
            receiveJobFailed.getAndSet(true)
          }
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(UnknownThrowable(new Exception()))
      // receive UnknownThrowable and JobFailed
      countDownLatch.await()
      assert(receiveJobFailed.get())
      eventLoop.unregisterListener(listener)
    }
  }


  test("post JobFailed event when receive class not found event") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
      KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val countDownLatch = new CountDownLatch(2)
      val receiveRunJob = new AtomicBoolean(false)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          if (event.isInstanceOf[RunJob]) {
            receiveRunJob.getAndSet(true)
          }
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new ClassNotFoundException()))
      // receive UnknownThrowable and JobFailed
      countDownLatch.await()
      assert(!receiveRunJob.get())
      eventLoop.unregisterListener(listener)
    }
  }

  test("post JobFailed event when receive oom event") {
    withEventLoop { eventLoop =>
      Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
      val env = KylinBuildEnv.getOrCreate(config)
      new JobMonitor(eventLoop)
      val countDownLatch = new CountDownLatch(2)
      val receiveRunJob = new AtomicBoolean(false)
      val listener = new KylinJobListener {
        override def onReceive(event: KylinJobEvent): Unit = {
          if (event.isInstanceOf[RunJob]) {
            receiveRunJob.getAndSet(true)
          }
          countDownLatch.countDown()
        }
      }
      eventLoop.registerListener(listener)
      eventLoop.post(ResourceLack(new OutOfMemoryError(s"Not enough memory to build and broadcast the table to " +
        s"all worker nodes. As a workaround, you can either disable broadcast by setting " +
        s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark driver " +
        s"memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value")))
      // receive UnknownThrowable and JobFailed
      countDownLatch.await()
      assert(env.sparkConf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).equals("-1"))
      assert(receiveRunJob.get())
      eventLoop.unregisterListener(listener)
    }
  }

}

class MockClusterManager extends IClusterManager {
  private var maxAllocation: ResourceInfo = _

  def setMaxAllocation(allocation: ResourceInfo): Unit = {
    maxAllocation = allocation
  }

  override def fetchMaximumResourceAllocation: ResourceInfo = maxAllocation

  override def fetchQueueAvailableResource(queueName: String): AvailableResource = null

  override def getBuildTrackingUrl(sparkSession: SparkSession): String = null

  override def killApplication(jobStepId: String): Unit = {}

  override def killApplication(jobStepPrefix: String, jobStepId: String): Unit = {}

  override def getRunningJobs(queues: util.Set[String]): util.List[String] = null

  override def fetchQueueStatistics(queueName: String): ResourceInfo = ResourceInfo(10, 20)

  override def isApplicationBeenKilled(applicationId: String): Boolean = false

  override def applicationExisted(jobId: String): Boolean = false

  override def getApplicationNameById(yarnAppId: Int): String = ""
}
