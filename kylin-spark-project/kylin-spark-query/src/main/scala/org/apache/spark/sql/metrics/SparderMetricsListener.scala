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

import org.apache.kylin.metrics.QuerySparkMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

class SparderMetricsListener() extends SparkListener with Logging {

  var stageJobMap: Map[Int, Int] = Map()
  var jobExecutionMap: Map[Int, QueryInformation] = Map()
  var executionInformationMap: Map[Long, ExecutionInformation] = Map()

  val queryExecutionMetrics = QuerySparkMetrics.getInstance()

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val executionIdString = event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    val sparderName = event.properties.getProperty("spark.app.name")
    val kylinQueryId = event.properties.getProperty("kylin.query.id")

    if (executionIdString == null || kylinQueryId == null) {
      logInfo(s"The job ${event.jobId} is not a query job.")
      return
    }

    val executionId = executionIdString.toLong

    if (executionInformationMap.apply(executionId).sparderName == null) {
      val executionInformation = new ExecutionInformation(kylinQueryId,
        executionInformationMap.apply(executionId).executionStartTime, sparderName)
      executionInformationMap += (executionId -> executionInformation)
    }

    jobExecutionMap += (event.jobId -> new QueryInformation(kylinQueryId, executionId))

    val stages = event.stageInfos.iterator
    while (stages.hasNext) {
      val stage: StageInfo = stages.next()
      stageJobMap += (stage.stageId -> event.jobId)
    }

    queryExecutionMetrics.onJobStart(kylinQueryId, sparderName, executionId,
      executionInformationMap.apply(executionId).executionStartTime, event.jobId, event.time)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    if (jobExecutionMap.contains(event.jobId)) {
      val isSuccess = event.jobResult match {
        case JobSucceeded => true
        case _ => false
      }
      queryExecutionMetrics.updateSparkJobMetrics(jobExecutionMap.apply(event.jobId).queryId, event.jobId, event.time,
        isSuccess)
      logInfo(s"The job ${event.jobId} has completed and the relevant metrics are updated to the cache")
      jobExecutionMap -= event.jobId
    }
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val queryId = event.properties.getProperty("kylin.query.id")
    val stageId = event.stageInfo.stageId

    if (stageJobMap.contains(stageId)) {
      val submitTime = event.stageInfo.submissionTime match {
        case Some(x) => x
        case None => -1
      }
      queryExecutionMetrics.onSparkStageStart(queryId, stageJobMap.apply(stageId), stageId, event.stageInfo.name, submitTime)
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stageInfo = event.stageInfo
    if (stageJobMap.contains(stageInfo.stageId) && jobExecutionMap.contains(stageJobMap.apply(stageInfo.stageId))) {
      val isSuccess = stageInfo.getStatusString match {
        case "succeeded" => true
        case _ => false
      }
      val stageMetrics = stageInfo.taskMetrics
      val sparkStageMetrics = new QuerySparkMetrics.SparkStageMetrics
      sparkStageMetrics.setMetrics(stageMetrics.resultSize, stageMetrics.executorDeserializeCpuTime,
        stageMetrics.executorDeserializeTime, stageMetrics.executorRunTime, stageMetrics.executorCpuTime,
        stageMetrics.jvmGCTime, stageMetrics.resultSerializationTime,
        stageMetrics.memoryBytesSpilled, stageMetrics.diskBytesSpilled, stageMetrics.peakExecutionMemory)
      queryExecutionMetrics.updateSparkStageMetrics(jobExecutionMap.apply(stageJobMap.apply(stageInfo.stageId)).queryId,
        stageJobMap.apply(stageInfo.stageId), stageInfo.stageId, isSuccess, sparkStageMetrics)
      stageJobMap -= stageInfo.stageId

      logInfo(s"The stage ${event.stageInfo.stageId} has completed and the relevant metrics are updated to the cache")
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart => onQueryExecutionStart(e)
      case e: SparkListenerSQLExecutionEnd => onQueryExecutionEnd(e)
      case _ => // Ignore
    }
  }

  private def onQueryExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    executionInformationMap += (event.executionId -> new ExecutionInformation(null, event.time, null))
  }

  private def onQueryExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val executionInformation = executionInformationMap.apply(event.executionId)
    queryExecutionMetrics.updateExecutionMetrics(executionInformation.queryId, event.time)
    executionInformationMap -= event.executionId
    logInfo(s"QueryExecution ${event.executionId} is completed at ${event.time} " +
      s"and the relevant metrics are updated to the cache")
  }
}

// ============================

class ExecutionInformation(
                            var queryId: String,
                            var executionStartTime: Long,
                            var sparderName: String
                          )

class QueryInformation(
                        val queryId: String,
                        val executionId: Long
                      )
