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
import org.apache.spark.sql.metrics.AppStatus
import org.apache.spark.utils.LogEx

/**
 * helper class for tracing the spark job execution time during query
 */
class SparkJobTrace(jobGroup: String,
                    queryTrace: QueryTrace,
                    sparkContext: SparkContext,
                    startAt: Long = System.currentTimeMillis()) extends LogEx {

  val appStatus = new AppStatus(sparkContext)

  /**
   * called right after job execution is done and the helper will calculate and estimate
   * durations for job execution steps (WAIT_FOR_EXECUTION, EXECUTION, FETCH_RESULT)
   *
   * As stages and tasks are executed in parallel, it is hard to have a precise duration
   * trace for each step
   * In this helper, we estimate the duration of WAIT_FOR_EXECUTION, EXECUTION, FETCH_RESULT
   * as follows
   * 1. Calculate the mean task launch delay, task execution duration and task fetch result time.
   * the launch delay = task launch time - stage submission time
   * 2. Sum the mean task launch delay, task execution duration and task fetch result time
   * from all stages. And calculate the proportion of each part
   * 3. Calculate the duration of each step by multiple the corresponding proportion and the
   * total job execution duration
   *
   * We use the mean of task launch delay as it can give a rough estimation on how much time
   * the tasks in a stage are spending on waiting for a free executor. And If the delay is
   * Long, it may imply the executor-core config is not insufficient for the number of tasks,
   * or the cluster is in heavy work load
   */
  def jobFinished(): Unit = {
    try {
      val jobDataSeq = appStatus.getJobData(jobGroup)

      if (jobDataSeq.isEmpty) {
        endAbnormalExecutionTrace()
        return
      }

      var jobExecutionTime = System.currentTimeMillis() - startAt
      val submissionTime = jobDataSeq.map(_.submissionTime).min
      if (submissionTime.isDefined) {
        queryTrace.amendLast(QueryTrace.PREPARE_AND_SUBMIT_JOB, submissionTime.get.getTime)
      }
      val completionTime = jobDataSeq.map(_.completionTime).max
      if (submissionTime.isDefined && completionTime.isDefined) {
        jobExecutionTime = completionTime.get.getTime - submissionTime.get.getTime
      }

      val jobMetrics = jobDataSeq.map(_.jobId)
        .flatMap(appStatus.getJobStagesSummary(_, 0.5))
        .foldLeft((0.0, 0.0)) { (acc, taskMetrics) =>
          (
            acc._1 + taskMetrics.executorRunTime.head + taskMetrics.executorDeserializeTime.head,
            acc._2 + taskMetrics.gettingResultTime.head
          )
        }
      val launchDelayTimeSum = jobDataSeq.flatMap(_.stageIds).flatMap(appStatus.getStage).map { stage =>
        appStatus.getTaskLaunchTime(stage.stageId(), 0.5) - stage.submissionTime()
      }.sum
      val sum = jobMetrics._1 + jobMetrics._2 + launchDelayTimeSum
      val computingTime = jobMetrics._1 * jobExecutionTime / sum
      val getResultTime = jobMetrics._2 * jobExecutionTime / sum
      val launchDelayTime = launchDelayTimeSum * jobExecutionTime / sum

      queryTrace.appendSpan(QueryTrace.WAIT_FOR_EXECUTION, launchDelayTime.longValue());
      queryTrace.appendSpan(QueryTrace.EXECUTION, computingTime.longValue());
      queryTrace.appendSpan(QueryTrace.FETCH_RESULT, getResultTime.longValue());
    } catch {
      case e =>
        logWarning(s"Failed trace spark job execution for $jobGroup", e)
        endAbnormalExecutionTrace()
    }
  }

  /**
   * called right after result transformation is done to count the
   * transformation time to total result fetch duration
   */
  def resultConverted(): Unit = {
    queryTrace.amendLast(QueryTrace.FETCH_RESULT, System.currentTimeMillis())
  }

  /**
   * add dummy spans for abnormal trace anyway
   */
  def endAbnormalExecutionTrace(): Unit = {
    queryTrace.appendSpan(QueryTrace.WAIT_FOR_EXECUTION, 0);
    queryTrace.appendSpan(QueryTrace.EXECUTION, System.currentTimeMillis() - startAt);
    queryTrace.appendSpan(QueryTrace.FETCH_RESULT, 0);
  }
}

