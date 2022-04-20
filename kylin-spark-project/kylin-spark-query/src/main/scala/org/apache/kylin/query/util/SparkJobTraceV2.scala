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

package org.apache.kylin.query.util

import org.apache.kylin.common.QueryTrace
import org.apache.spark.SparkContext

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

class SparkJobTraceV2(jobGroup: String,
                      queryTrace: QueryTrace,
                      sparkContext: SparkContext,
                      zoneId: ZoneId,
                      detailEnabled: Boolean = false,
                      startAt: Long = System.currentTimeMillis()) extends SparkJobTrace(jobGroup: String,
  queryTrace: QueryTrace,
  sparkContext: SparkContext,
  startAt: Long) {

  val PATTERN = "HH:mm:ss.SSS"
  val bytesRead = "bytesRead"
  val stageInfo = "stageInfo"
  val runningTime = "runningTime"
  val launchTime = "launchTime"


  def dateStr(ts: Long): String = {
    new Date(ts).toInstant.atZone(zoneId).toLocalDateTime.format(DateTimeFormatter.ofPattern(PATTERN, Locale.ROOT))
  }

  def dateStr(ts: java.util.Date): String = {
    ts.toInstant.atZone(zoneId).toLocalDateTime.format(DateTimeFormatter.ofPattern(PATTERN, Locale.ROOT))
  }

  override def jobFinished(): Unit = {
    logDebug("Query job finished.")
    val finishedAt = System.currentTimeMillis()
    try {
      val jobDataSeq = appStatus.getJobData(jobGroup)
      val firstSubmissionTime = jobDataSeq.map(_.submissionTime).min
      val lastCompletionTime = jobDataSeq.map(_.completionTime).max
      var firstLaunchTime = -1L

      if (jobDataSeq.isEmpty) {
        endAbnormalExecutionTrace()
        return
      }

      jobDataSeq.foreach(
        job => {
          val submissionTime = dateStr(job.submissionTime.getOrElse(new java.util.Date(0)))
          val completionTime = dateStr(job.completionTime.getOrElse(new java.util.Date(0)))
          val killedDesc = if (job.killedTasksSummary.isEmpty) {
            "EMPTY"
          } else {
            job.killedTasksSummary.mkString("|")
          }
          val stageMetrics = job.stageIds.flatMap(stageId => appStatus.calStageMetrics(stageId)).map(
            stage => if (stage(bytesRead).nonEmpty && stage(runningTime).nonEmpty) {
              if (firstLaunchTime == -1 || stage(launchTime).apply(0) < firstLaunchTime) firstLaunchTime = stage(launchTime).apply(0)
              ("Stage:%d,%d, launch time of first and last task are %s and %s, " +
                " bytesRead is [%s], runningTime is [%s]").format(
                stage(stageInfo).apply(0),
                stage(stageInfo).apply(1),
                dateStr(stage(launchTime).apply(0)),
                dateStr(stage(launchTime).apply(1)),
                stage(bytesRead).mkString(","),
                stage(runningTime).mkString(", ")
              )
            } else {
              ("Stage:%d,%d").format(stage(stageInfo).apply(0), stage(stageInfo).apply(1))
            }).mkString(";")

          logInfo(
            s"Job ${job.jobId} is submitted at ${submissionTime} and completed at ${completionTime}.It has ${job.numTasks} tasks, " +
              s"succeed ${job.numCompletedTasks} tasks, ${job.numFailedTasks} failed tasks,${job.numKilledTasks} killed tasks. " +
              s"Killed tasks info: ${killedDesc}. Stages ${stageMetrics}.")
        }
      )

      var jobExecutionTimeByKylin = finishedAt - startAt
      if (firstSubmissionTime.isDefined) {
        queryTrace.amendLast(QueryTrace.PREPARE_AND_SUBMIT_JOB, firstSubmissionTime.get.getTime)
      } else {
        logInfo("No firstSubmissionTime")
      }
      if (firstSubmissionTime.isDefined && lastCompletionTime.isDefined) {
        val jobExecutionTimeBySpark = lastCompletionTime.get.getTime - firstSubmissionTime.get.getTime
        logInfo(s"jobExecutionTime will change from ${jobExecutionTimeByKylin} to ${jobExecutionTimeBySpark} .")
        logInfo(s"Kylin submit query at ${dateStr(startAt)}" +
          s", first spark job submitted at ${dateStr(firstSubmissionTime.get.getTime)}" +
          s", last spark job finished at ${dateStr(lastCompletionTime.get.getTime)}" +
          s", query finished at ${dateStr(finishedAt)} .")
        jobExecutionTimeByKylin = jobExecutionTimeBySpark

        queryTrace.appendSpan(QueryTrace.WAIT_FOR_EXECUTION, firstLaunchTime - firstSubmissionTime.get.getTime)
        queryTrace.appendSpan(QueryTrace.EXECUTION, lastCompletionTime.get.getTime - firstLaunchTime)
        queryTrace.appendSpan(QueryTrace.FETCH_RESULT, finishedAt.longValue - lastCompletionTime.get.getTime)
        queryTrace.appendSpan(QueryTrace.CALCULATE_STAT, System.currentTimeMillis - finishedAt)
      } else {
        logInfo("No firstSubmissionTime or lastCompletionTime")
      }
    } catch {
      case e =>
        logWarning(s"Failed trace spark job execution for $jobGroup", e)
        endAbnormalExecutionTrace()
    }
  }
}

