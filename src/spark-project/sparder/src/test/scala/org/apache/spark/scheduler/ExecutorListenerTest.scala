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

package org.apache.spark.scheduler

import org.apache.kylin.common.KylinConfig
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.storage._
import org.apache.spark.util.ManualClock
import org.apache.spark.{ExecutorAllocationClient, Success, TaskResultLost, TaskState}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mock, when}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

class ExecutorListenerTest extends SparderBaseFunSuite with SharedSparkSession {


  private var kylinConfig: KylinConfig = _
  private var client: ExecutorAllocationClient = _
  private var listener: ExecutorListener = _
  private var clock: ManualClock = _

  private val idleTimeoutNs = TimeUnit.SECONDS.toNanos(10L)
  private val shuffleTimeoutNs = TimeUnit.SECONDS.toNanos(30L)

  private def idleDeadline: Long = clock.nanoTime() + idleTimeoutNs + 1

  private def storageDeadline: Long = clock.nanoTime() + shuffleTimeoutNs + 1

  private def shuffleDeadline: Long = clock.nanoTime() + shuffleTimeoutNs + 1

  // List of known executors. Allows easily mocking which executors are alive without
  // having to use mockito APIs directly in each test.
  private val knownExecs = mutable.HashSet[String]()

  private val execInfo = new ExecutorInfo("host1", 1, Map.empty,
    Map.empty, Map.empty, DEFAULT_RESOURCE_PROFILE_ID)

  override def beforeEach(): Unit = {
    super.beforeEach()
    knownExecs.clear()
    knownExecs += "1"
    clock = new ManualClock()
    client = mock(classOf[ExecutorAllocationClient])
    when(client.isExecutorActive(any())).thenAnswer { invocation =>
      knownExecs.contains(invocation.getArguments()(0).asInstanceOf[String])
    }
    kylinConfig = mock(classOf[KylinConfig])
    when(kylinConfig.getExecutorIdleTimeout).thenAnswer { _ =>
      10
    }
    when(kylinConfig.getShuffleTrackingTimeout).thenAnswer { _ =>
      30
    }
    listener = new ExecutorListener(kylinConfig, client, mockListenerBus(), clock)
  }

  test("exception testing") {

    listener.onOtherEvent(mock(classOf[SparkListenerEvent]))
    listener.executorsKilled(Seq("id"))
    listener.onTaskStart(SparkListenerTaskStart(1, 0, taskInfo("id", 1)))
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "foo", TaskResultLost, taskInfo("id", 1),
      new ExecutorMetrics, null))
    assert(listener.timedOutExecutors().isEmpty)
    assert(listener.noPendingRemovalExecutors().isEmpty)

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(clock.getTimeMillis(), "id", "test"))
    assert(listener.executorCount === 0)
    listener.onBlockUpdated(rddUpdate(111, 0, "1"))
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(12))
    assert(listener.executorCount === 1)


    listener.onTaskStart(SparkListenerTaskStart(1, 0, taskInfo("1", 1)))
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "foo", TaskResultLost, taskInfo("1", 1),
      new ExecutorMetrics, null))
    assert(listener.isExecutorIdle("1"))
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(BlockManagerId("1", "1.example.com", 42),
        ShuffleDataBlockId(111, 0, 0), StorageLevel.MEMORY_ONLY, 1L, 0L)))
    assert(!listener.isExecutorIdle("1"))

  }

  test("basic testing") {
    listener.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "driver", execInfo))
    listener.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(listener.executorCount === 1)
    assert(listener.isExecutorIdle("1"))
    assert(listener.timedOutExecutors(-1).isEmpty)
    assert(listener.timedOutExecutors(idleDeadline) === Seq("1"))
    assert(listener.timedOutExecutors().isEmpty)
    assert(listener.noPendingRemovalExecutors() === Seq("1"))
    listener.executorsKilled(listener.noPendingRemovalExecutors())
    assert(listener.noPendingRemovalExecutors().isEmpty)
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(clock.getTimeMillis(), "driver", "test"))
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(clock.getTimeMillis(), "1", "test"))
    assert(listener.executorCount === 0)

  }

  test("use appropriate time out depending on whether blocks are stored") {
    knownExecs += "1"
    listener.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(listener.isExecutorIdle("1"))
    assert(listener.timedOutExecutors(idleDeadline) === Seq("1"))

    listener.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(listener.isExecutorIdle("1"))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    assert(listener.timedOutExecutors(storageDeadline) === Seq("1"))

    listener.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.NONE))
    assert(listener.isExecutorIdle("1"))
    assert(listener.timedOutExecutors(idleDeadline) === Seq("1"))

    listener.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("1", 1)))
    assert(!listener.isExecutorIdle("1"))
    listener.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(!listener.isExecutorIdle("1"))
    listener.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.NONE))
    assert(!listener.isExecutorIdle("1"))

  }

  test("keeps track of stored blocks for each rdd and split") {
    knownExecs ++= Set("1", "2")

    listener.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))

    listener.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    assert(listener.timedOutExecutors(storageDeadline) === Seq("1"))

    listener.onBlockUpdated(rddUpdate(1, 1, "1"))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    assert(listener.timedOutExecutors(storageDeadline) === Seq("1"))

    listener.onBlockUpdated(rddUpdate(2, 0, "1"))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    assert(listener.timedOutExecutors(storageDeadline) === Seq("1"))

    listener.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.NONE))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    assert(listener.timedOutExecutors(storageDeadline) === Seq("1"))

    listener.onUnpersistRDD(SparkListenerUnpersistRDD(1))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    assert(listener.timedOutExecutors(storageDeadline) === Seq("1"))

    // Make sure that if we get an unpersist event much later, which moves an executor from having
    // cached blocks to no longer having cached blocks, it will time out based on the time it
    // originally went idle.
    clock.setTime(idleDeadline)
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(2))
    assert(listener.timedOutExecutors(clock.nanoTime()) === Seq("1"))
  }

  test("shuffle block tracking") {

    // 3 jobs: 2 and 3 share a shuffle, 1 has a separate shuffle.
    val stage1 = stageInfo(1, shuffleId = 0)
    val stage2 = stageInfo(2)

    val stage3 = stageInfo(3, shuffleId = 1)
    val stage4 = stageInfo(4)

    val stage5 = stageInfo(5, shuffleId = 1)
    val stage6 = stageInfo(6)

    // Start jobs 1 and 2. Finish a task on each, but don't finish the jobs. This should prevent the
    // executor from going idle since there are active shuffles.
    listener.onJobStart(SparkListenerJobStart(1, clock.getTimeMillis(), Seq(stage1, stage2)))
    listener.onJobStart(SparkListenerJobStart(2, clock.getTimeMillis(), Seq(stage3, stage4)))

    listener.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(listener.timedOutExecutors(idleDeadline) === Seq("1"))

    // First a failed task, to make sure it does not count.
    listener.onTaskStart(SparkListenerTaskStart(1, 0, taskInfo("1", 1)))
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "foo", TaskResultLost, taskInfo("1", 1),
      new ExecutorMetrics, null))
    assert(listener.timedOutExecutors(idleDeadline) === Seq("1"))

    listener.onTaskStart(SparkListenerTaskStart(1, 0, taskInfo("1", 1)))
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "foo", Success, taskInfo("1", 1),
      new ExecutorMetrics, null))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)

    listener.onTaskStart(SparkListenerTaskStart(3, 0, taskInfo("1", 1)))
    listener.onTaskEnd(SparkListenerTaskEnd(3, 0, "foo", Success, taskInfo("1", 1),
      new ExecutorMetrics, null))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)

    // Finish the jobs, now the executor should be idle, but with the shuffle timeout, since the
    // shuffles are not active.
    listener.onJobEnd(SparkListenerJobEnd(1, clock.getTimeMillis(), JobSucceeded))
    assert(!listener.isExecutorIdle("1"))

    listener.onJobEnd(SparkListenerJobEnd(2, clock.getTimeMillis(), JobSucceeded))
    assert(listener.isExecutorIdle("1"))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    assert(listener.timedOutExecutors(storageDeadline) === Seq("1"))
    assert(listener.timedOutExecutors(shuffleDeadline) === Seq("1"))

    // Start job 3. Since it shares a shuffle with job 2, the executor should not be considered
    // idle anymore, even if no tasks are run.
    listener.onJobStart(SparkListenerJobStart(3, clock.getTimeMillis(), Seq(stage5, stage6)))
    assert(!listener.isExecutorIdle("1"))
    assert(listener.timedOutExecutors(shuffleDeadline).isEmpty)

    listener.onJobEnd(SparkListenerJobEnd(3, clock.getTimeMillis(), JobSucceeded))
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    assert(listener.timedOutExecutors(shuffleDeadline) === Seq("1"))

    // Clean up the shuffles, executor now should now time out at the idle deadline.
    listener.shuffleCleaned(0)
    assert(listener.timedOutExecutors(idleDeadline).isEmpty)
    listener.shuffleCleaned(1)
    assert(listener.timedOutExecutors(idleDeadline) === Seq("1"))

  }

  private def stageInfo(id: Int, shuffleId: Int = -1): StageInfo = {
    new StageInfo(id, 0, s"stage$id", 1, Nil, Nil, "",
      shuffleDepId = if (shuffleId >= 0) Some(shuffleId) else None,
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
  }

  private def taskInfo(
                        execId: String,
                        id: Int,
                        speculative: Boolean = false,
                        duration: Long = -1L): TaskInfo = {
    val start = if (duration > 0) clock.getTimeMillis() - duration else clock.getTimeMillis()
    val task = new TaskInfo(
      id, id, 1, id, start, execId, "foo.example.com",
      TaskLocality.PROCESS_LOCAL, speculative)
    if (duration > 0) {
      task.markFinished(TaskState.FINISHED, math.max(1, clock.getTimeMillis()))
    }
    task
  }

  private def rddUpdate(
                         rddId: Int,
                         splitIndex: Int,
                         execId: String,
                         level: StorageLevel = StorageLevel.MEMORY_ONLY): SparkListenerBlockUpdated = {
    SparkListenerBlockUpdated(
      BlockUpdatedInfo(BlockManagerId(execId, "1.example.com", 42),
        RDDBlockId(rddId, splitIndex), level, 1L, 0L))
  }

  private def mockListenerBus(): LiveListenerBus = {
    val bus = mock(classOf[LiveListenerBus])
    doAnswer { invocation =>
      listener.onOtherEvent(invocation.getArguments()(0).asInstanceOf[SparkListenerEvent])
    }.when(bus).post(any())
    bus
  }
}
