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
import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv
import org.apache.spark.status.{JobDataWrapper, StageDataWrapper, TaskDataWrapper}

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.JavaConverters._

object SparkUIZombieJobCleaner extends Logging {
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor
  private val zombieJobCleanSeconds = KylinConfig.getInstanceFromEnv.getSparkUIZombieJobCleanSeconds

  def regularClean(): Unit = {
    logInfo(s"${System.identityHashCode(this)}: Start clean zombie job.")
    scheduler.scheduleWithFixedDelay(clean, 10, zombieJobCleanSeconds, TimeUnit.SECONDS)
  }

  private def clean = {
    new Runnable {
      override def run(): Unit = {
        try {
          if (SparderEnv.isSparkAvailable) {
            logInfo(s"clean zombie job once.")
            val appStatusStore = SparderEnv.getSparkSession.sparkContext.statusStore

            val sparkUIActiveJobIds = appStatusStore.store.view(classOf[JobDataWrapper]).asScala.filter { jobData =>
              isActiveJobOnUI(jobData.info.status)
            }.map(_.info.jobId)

            val allActualActiveJobIds = SparderEnv.getSparkSession.sparkContext.dagScheduler.activeJobs.map(_.jobId)

            val toBeDeleteJobIds = sparkUIActiveJobIds.filter(id => !allActualActiveJobIds.contains(id))

            val toBeDeleteStageIds = appStatusStore.store.view(classOf[JobDataWrapper]).asScala.filter { jobData =>
              toBeDeleteJobIds.toSeq.contains(jobData.info.jobId)
            }.flatMap(_.info.stageIds)

            val toBeDeleteStageIdKeys = appStatusStore.store.view(classOf[StageDataWrapper])
              .asScala.filter { stageData => toBeDeleteStageIds.toSeq.contains(stageData.info.stageId)
            }.map { stageData => Array(stageData.info.stageId, stageData.info.attemptId) }

            val toBeDeleteTaskIds = toBeDeleteStageIdKeys.flatMap { stage =>
              appStatusStore.taskList(stage(0), stage(1), Integer.MAX_VALUE).map(_.taskId)
            }

            logInfo(s"be delete job ids is: ${toBeDeleteJobIds}")
            toBeDeleteJobIds.foreach { id => appStatusStore.store.delete(classOf[JobDataWrapper], id) }
            logInfo(s"be delete stage ids is: ${toBeDeleteStageIds}")
            toBeDeleteStageIdKeys.foreach { id => appStatusStore.store.delete(classOf[StageDataWrapper], id) }
            logInfo(s"be delete tasks ids is: ${toBeDeleteTaskIds}")
            toBeDeleteTaskIds.foreach { id => appStatusStore.store.delete(classOf[TaskDataWrapper], id) }
          }
        } catch {
          case th: Throwable => logError("Error when clean spark zombie job.", th)
        }
      }
    }
  }

  /**
   * job status not SUCCEEDED and FAILED will show as active job.
   * see org.apache.spark.ui.jobs.AllJobsPage#render
   *
   * @param status
   */
  private def isActiveJobOnUI(status: JobExecutionStatus): Boolean = {
    status != JobExecutionStatus.SUCCEEDED && status != JobExecutionStatus.FAILED
  }
}
