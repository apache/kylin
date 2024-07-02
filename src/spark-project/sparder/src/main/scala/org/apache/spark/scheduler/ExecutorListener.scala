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
import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{RDDBlockId, ShuffleDataBlockId}
import org.apache.spark.util.Clock

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.collection.JavaConverters._
import scala.collection.mutable

class ExecutorListener(kylinConfig: KylinConfig,
                       client: ExecutorAllocationClient,
                       listenerBus: LiveListenerBus,
                       clock: Clock)
  extends SparkListener with CleanerListener with Logging {

  private val idleTimeoutNs = TimeUnit.SECONDS.toNanos(kylinConfig.getExecutorIdleTimeout)
  private val shuffleTimeoutNs = TimeUnit.SECONDS.toNanos(kylinConfig.getShuffleTrackingTimeout)

  private val executors = new ConcurrentHashMap[String, Tracker]()

  private val shuffleToActiveJobs = new mutable.HashMap[Int, mutable.ArrayBuffer[Int]]()
  private val stageToShuffleID = new mutable.HashMap[Int, Int]()
  private val jobToStageIDs = new mutable.HashMap[Int, Seq[Int]]()
  private val driverId = "driver"


  // Visible for testing.
  private[scheduler] def isExecutorIdle(id: String): Boolean = {
    Option(executors.get(id))
      .map(_.isIdle).getOrElse(throw SparkCoreErrors.noExecutorIdleError(id))
  }

  // Visible for testing
  private[scheduler] def timedOutExecutors(when: Long): Seq[String] = {
    executors.asScala.flatMap { case (id, exec) =>
      if (exec.isIdle && exec.timeoutAt <= when && !id.equalsIgnoreCase(driverId)) Some(id) else None
    }.toSeq
  }

  def timedOutExecutors(): Seq[String] = {
    val now = clock.nanoTime()
    executors.asScala
      .filter { case (id, exec) =>
        !exec.pendingRemoval && !exec.hasActiveShuffle && !id.equalsIgnoreCase(driverId)
      }
      .filter(_._2.timeoutAt < now).keys
      .toSeq
  }

  def noPendingRemovalExecutors(): Seq[String] = {
    executors.asScala
      .filter { case (id, exec) =>
        !exec.pendingRemoval && !id.equalsIgnoreCase(driverId)
      }.keys
      .toSeq
  }

  def executorsKilled(ids: Seq[String]): Unit = {
    ids.foreach { id =>
      val tracker = executors.get(id)
      if (tracker != null) {
        tracker.pendingRemoval = true
      }
    }
  }

  def executorCount: Int = {
    executors.asScala.keys.filter { name =>
      kylinConfig.isUTEnv || !name.equalsIgnoreCase(driverId)
    }.toSeq.size
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val shuffleStages = event.stageInfos.flatMap { s =>
      s.shuffleDepId.toSeq.map { shuffleId =>
        s.stageId -> shuffleId
      }
    }
    var updateExecutors = false
    shuffleStages.foreach { case (stageId, shuffle) =>
      val jobIDs = shuffleToActiveJobs.get(shuffle) match {
        case Some(jobs) =>
          // If a shuffle is being re-used, we need to re-scan the executors and update their
          // tracker with the information that the shuffle data they're storing is in use.
          logDebug(s"Reusing shuffle $shuffle in job ${event.jobId}.")
          updateExecutors = true
          jobs

        case _ =>
          logDebug(s"Registered new shuffle $shuffle (from stage $stageId).")
          val jobs = new mutable.ArrayBuffer[Int]()
          shuffleToActiveJobs(shuffle) = jobs
          jobs
      }
      jobIDs += event.jobId
    }

    if (updateExecutors) {
      val activeShuffleIds = shuffleStages.map(_._2)
      executors.values().asScala.foreach { exec =>
        if (!exec.hasActiveShuffle) {
          exec.updateActiveShuffles(activeShuffleIds)
        }
      }
    }

    stageToShuffleID ++= shuffleStages
    jobToStageIDs(event.jobId) = shuffleStages.map(_._1)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    var updateExecutors = false
    val activeShuffles = new mutable.ArrayBuffer[Int]()
    shuffleToActiveJobs.foreach { case (shuffleId, jobs) =>
      jobs -= event.jobId
      if (jobs.nonEmpty) {
        activeShuffles += shuffleId
      } else {
        // If a shuffle went idle we need to update all executors to make sure they're correctly
        // tracking active shuffles.
        updateExecutors = true
      }
    }

    if (updateExecutors) {
      executors.values().asScala.foreach { exec =>
        if (exec.hasActiveShuffle) {
          exec.updateActiveShuffles(activeShuffles)
        }
      }
    }

    jobToStageIDs.remove(event.jobId).foreach { stages =>
      stages.foreach { id => stageToShuffleID -= id }
    }
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    val executorId = event.taskInfo.executorId
    if (client.isExecutorActive(executorId)) {
      val exec = ensureExecutorIsTracked(executorId)
      exec.updateRunningTasks(1)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val executorId = event.taskInfo.executorId
    val exec = executors.get(executorId)
    if (exec != null) {
      // If the task succeeded and the stage generates shuffle data, record that this executor
      // holds data for the shuffle. This code will track all executors that generate shuffle
      // for the stage, even if speculative tasks generate duplicate shuffle data and end up
      // being ignored by the map output tracker.
      //
      // This means that an executor may be marked as having shuffle data, and thus prevented
      // from being removed, even though the data may not be used.
      event.reason match {
        case Success => stageToShuffleID.get(event.stageId).foreach { shuffleId =>
          exec.addShuffle(shuffleId)
        }
        case _ =>
      }

      // Update the number of running tasks after checking for shuffle data, so that the shuffle
      // information is up-to-date in case the executor is going idle.
      exec.updateRunningTasks(-1)
    }
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val exec = ensureExecutorIsTracked(event.executorId)
    exec.updateRunningTasks(0)
    logInfo(s"New executor ${event.executorId} has registered (new total is ${executors.size()})")
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    val removed = executors.remove(event.executorId)
    if (removed != null) {
      logInfo(s"Executor ${event.executorId} removed (new total is ${executors.size()})")
    }
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    val exec = ensureExecutorIsTracked(event.blockUpdatedInfo.blockManagerId.executorId)

    // Check if it is a shuffle file, or RDD to pick the correct codepath for update
    if (!event.blockUpdatedInfo.blockId.isInstanceOf[RDDBlockId]) {
      if (event.blockUpdatedInfo.blockId.isInstanceOf[ShuffleDataBlockId]) {
        /**
         * The executor monitor keeps track of locations of cache and shuffle blocks and this can
         * be used to decide which executor(s) Spark should shutdown first. Since we move shuffle
         * blocks around now this wires it up so that it keeps track of it. We only do this for
         * data blocks as index and other blocks blocks do not necessarily mean the entire block
         * has been committed.
         */
        event.blockUpdatedInfo.blockId match {
          case ShuffleDataBlockId(shuffleId, _, _) => exec.addShuffle(shuffleId)
          case _ => // For now we only update on data blocks
        }
      }
      return
    }
    val storageLevel = event.blockUpdatedInfo.storageLevel
    val blockId = event.blockUpdatedInfo.blockId.asInstanceOf[RDDBlockId]
    if (storageLevel.isValid && !storageLevel.useDisk) {
      val hadCachedBlocks = exec.cachedBlocks.nonEmpty
      val blocks = exec.cachedBlocks.getOrElseUpdate(blockId.rddId,
        new mutable.BitSet(blockId.splitIndex))
      blocks += blockId.splitIndex
      if (!hadCachedBlocks) {
        exec.updateTimeout()
      }
    } else {
      exec.cachedBlocks.get(blockId.rddId).foreach { blocks =>
        blocks -= blockId.splitIndex
        if (blocks.isEmpty) {
          cleanCachedBlocks(exec, blockId.rddId)
        }
      }
    }
  }

  private def cleanCachedBlocks(exec: Tracker, rddId: Int): Unit = {
    exec.cachedBlocks -= rddId
    if (exec.cachedBlocks.isEmpty) {
      exec.updateTimeout()
    }
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    executors.values().asScala.foreach { exec =>
      cleanCachedBlocks(exec, event.rddId)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case ShuffleCleanedEvent(id) => cleanupShuffle(id)
    case _ =>
  }

  override def rddCleaned(rddId: Int): Unit = {
    /** Don't need */
  }

  override def shuffleCleaned(shuffleId: Int): Unit = {
    listenerBus.post(ShuffleCleanedEvent(shuffleId))
  }

  override def broadcastCleaned(broadcastId: Long): Unit = {
    /** Don't need */
  }

  override def accumCleaned(accId: Long): Unit = {
    /** Don't need */
  }

  override def checkpointCleaned(rddId: Long): Unit = {
    /** Don't need */
  }


  def ensureExecutorIsTracked(id: String): Tracker = {
    executors.computeIfAbsent(id, _ => new Tracker())
  }

  private def cleanupShuffle(id: Int): Unit = {
    logDebug(s"Cleaning up state related to shuffle $id.")
    shuffleToActiveJobs -= id
    executors.asScala.values.foreach(_.removeShuffle(id))
  }

  class Tracker() {
    @volatile var timeoutAt: Long = Long.MaxValue
    var pendingRemoval: Boolean = false
    var hasActiveShuffle: Boolean = false

    private var idleStart: Long = -1
    private var runningTasks: Int = 0

    // Maps RDD IDs to the partition IDs stored in the executor.
    // This should only be used in the event thread.
    val cachedBlocks = new mutable.HashMap[Int, mutable.BitSet]()
    val shuffleIds = new mutable.HashSet[Int]()

    def isIdle: Boolean = idleStart >= 0 && !hasActiveShuffle

    def updateRunningTasks(delta: Int): Unit = {
      runningTasks = math.max(0, runningTasks + delta)
      idleStart = if (runningTasks == 0) clock.nanoTime() else -1L
      updateTimeout()
    }

    def updateTimeout(): Unit = {
      val newDeadline = if (idleStart >= 0) {
        val timeout = if (cachedBlocks.nonEmpty || shuffleIds.nonEmpty) {
          shuffleTimeoutNs
        } else {
          idleTimeoutNs
        }
        val deadline = idleStart + timeout
        if (deadline >= 0) deadline else Long.MaxValue
      } else {
        Long.MaxValue
      }
      timeoutAt = newDeadline
    }

    def addShuffle(id: Int): Unit = {
      if (shuffleIds.add(id)) {
        hasActiveShuffle = true
      }
    }

    def removeShuffle(id: Int): Unit = {
      if (shuffleIds.remove(id) && shuffleIds.isEmpty) {
        hasActiveShuffle = false
        if (isIdle) {
          updateTimeout()
        }
      }
    }

    def updateActiveShuffles(ids: Iterable[Int]): Unit = {
      val hadActiveShuffle = hasActiveShuffle
      hasActiveShuffle = ids.exists(shuffleIds.contains)
      if (hadActiveShuffle && isIdle) {
        updateTimeout()
      }
    }
  }

  private case class ShuffleCleanedEvent(id: Int) extends SparkListenerEvent {
    override protected[spark] def logEvent: Boolean = false
  }

}