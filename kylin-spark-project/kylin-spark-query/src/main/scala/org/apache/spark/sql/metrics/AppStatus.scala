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

package org.apache.spark.sql.metrics

import org.apache.spark.status.api.v1
import org.apache.spark.status.{TaskDataWrapper, TaskIndexNames}
import org.apache.spark.util.Utils
import org.apache.spark.utils.LogEx
import org.apache.spark.{SparkContext, SparkStageInfo}

class AppStatus(sparkContext: SparkContext) extends LogEx {

  val defaultUnsortedQuantiles: Array[Double] = Array(0, 0.25, 0.5, 0.75, 1.0)

  def calStageMetrics(stageId: Int, unsortedQuantiles: Array[Double] = defaultUnsortedQuantiles):
  Seq[Map[String, IndexedSeq[Long]]] = {
    val stageDataList: Seq[v1.StageData] = sparkContext.statusStore.stageData(stageId)
    stageDataList.map(s => {

      // 1. Get launch time of first and last task
      val stageKey = Array(s.stageId, s.attemptId)
      val taskIterator = sparkContext.statusStore.store.view(classOf[TaskDataWrapper])
        .parent(stageKey)
        .index(TaskIndexNames.LAUNCH_TIME)
        .iterator()

      var firstLaunchTime = -1L
      var lastLaunchTime = -1L
      while (taskIterator.hasNext) {
        val lt = taskIterator.next().launchTime
        if (firstLaunchTime == -1) {
          firstLaunchTime = lt
        } else if (lt < firstLaunchTime) {
          firstLaunchTime = lt
        }
        if (lastLaunchTime == -1) {
          lastLaunchTime = lt
        } else if (lt > lastLaunchTime) {
          lastLaunchTime = lt
        }
      }

      // 2. Get distribution of bytesRead & runningTime
      val metricsDistribution = sparkContext.statusStore.taskSummary(s.stageId, s.attemptId, unsortedQuantiles)
      val bytesRead: IndexedSeq[Long] = if (metricsDistribution.nonEmpty) {
        metricsDistribution.get.inputMetrics.bytesRead.map(x => x.toLong)
      } else {
        IndexedSeq.empty
      }

      val runningTime: IndexedSeq[Long] = if (metricsDistribution.nonEmpty) {
        metricsDistribution.get.executorRunTime.map(x => x.toLong)
      } else {
        IndexedSeq.empty
      }

      Map[String, IndexedSeq[Long]](
        "stageInfo" -> IndexedSeq(s.stageId, s.attemptId),
        "launchTime" -> IndexedSeq(firstLaunchTime, lastLaunchTime),
        "bytesRead" -> bytesRead,
        "runningTime" -> runningTime
      )
    }).seq
  }

  def getTaskLaunchTime(stageId: Int, quantile: Double): Double = {
    scanTasks(stageId, TaskIndexNames.LAUNCH_TIME, quantile) { t => t.launchTime }
  }

  // copied from org.apache.spark.status.AppStatusStore.taskSummary
  def scanTasks(stageId: Int, index: String, quantile: Double)(fn: TaskDataWrapper => Long): Double = {
    val stageKey = Array(stageId, 0)
    val count = {
      Utils.tryWithResource(
        sparkContext.statusStore.store.view(classOf[TaskDataWrapper])
          .parent(stageKey)
          .index(TaskIndexNames.EXEC_RUN_TIME)
          .first(0L)
          .closeableIterator()
      ) { it =>
        var _count = 0L
        while (it.hasNext()) {
          _count += 1
          it.skip(1)
        }
        _count
      }
    }

    val idx = math.min((quantile * count).toLong, count - 1)
    Utils.tryWithResource(
      sparkContext.statusStore.store.view(classOf[TaskDataWrapper])
        .parent(stageKey)
        .index(index)
        .first(0L)
        .closeableIterator()
    ) { it =>
      var last = Double.NaN
      var currentIdx = -1L
      if (idx == currentIdx) {
        last
      } else {
        val diff = idx - currentIdx
        currentIdx = idx
        if (it.skip(diff - 1)) {
          last = fn(it.next()).toDouble
          last
        } else {
          Double.NaN
        }
      }
    }
  }

  def getJobStagesSummary(jobId: Int, quantile: Double): Seq[v1.TaskMetricDistributions] = {
    getJobData(jobId).map { jobData =>
      jobData.stageIds.flatMap { stageId =>
        sparkContext.statusStore.taskSummary(stageId, 0, Array(quantile))
      }
    }.getOrElse(Seq.empty)
  }

  def getStage(stageId: Int): Option[SparkStageInfo] = {
    sparkContext.statusTracker.getStageInfo(stageId)
  }

  def getJobData(jobGroup: String): Seq[v1.JobData] = {
    sparkContext.statusTracker.getJobIdsForGroup(jobGroup).map(getJobData).filter(_.isDefined).map(_.get)
  }

  def getJobData(jobId: Int): Option[v1.JobData] = {
    try {
      Some(sparkContext.statusStore.job(jobId))
    } catch {
      case _: NoSuchElementException => None
    }
  }
}

