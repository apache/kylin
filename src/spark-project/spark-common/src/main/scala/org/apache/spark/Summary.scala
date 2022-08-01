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

package org.apache.spark

import org.apache.spark.status.api.v1._

sealed trait Summary {
  val separator = "\t"

  def csvFormat: Seq[String]
}

class JobSummary(
                  val jobId: Int,
                  val name: String,
                  val jobGroup: String,
                  val jobStatus: String,
                  val stages: Seq[StageSummary]
                ) extends Summary {
  require(stages.nonEmpty)

  override def toString: String = {
    val builder = new StringBuilder
    builder.append(s"\nJobSummary(jobId=$jobId, name=$name, jobGroup=$jobGroup, status=$jobStatus, stages=(")
    stages.foreach(stage =>
      builder.append(s"\n\t\t$stage")
    )
    builder.append("))")
    builder.toString()
  }

  def csvFormat: Seq[String] = {
    val jobSummary = Seq(jobId, name, jobGroup, jobStatus).mkString(separator)
    val stagesSummary = stages.flatMap(_.csvFormat)
    stagesSummary.map(jobSummary + separator + _)
  }
}

object JobSummary {
  def apply(job: JobData, stages: Seq[StageSummary]): JobSummary = {
    val _stages = if (stages.isEmpty) {
      Seq(StageSummary.invalid)
    } else {
      stages
    }
    new JobSummary(job.jobId, job.name, job.jobGroup.getOrElse("null"), job.status.toString, _stages)
  }

  def schema(): Seq[String] = {
    Seq("jobId", "name", "jobGroup", "jobStatus")
  }
}


class StageSummary(
                    val stageId: Int,
                    val attemptId: Int,
                    val stageStatus: String,
                    val executorRunTime: Long,
                    val executorCpuTime: Long,
                    val stageInputBytes: Long,
                    val stageInputRecords: Long,
                    val stageOutputBytes: Long,
                    val stageOutputRecords: Long,
                    val shuffleReadBytes: Long,
                    val shuffleReadRecords: Long,
                    val shuffleWriteBytes: Long,
                    val shuffleWriteRecords: Long,
                    val tasks: Seq[TaskSummary]
                  ) extends Summary {
  require(tasks.nonEmpty)

  override def toString: String = {
    val builder = new StringBuilder
    builder.append(s"StageSummary(stageId=$stageId, attemptId=$attemptId, status=$stageStatus, executorRunTime=$executorRunTime, " +
      s"executorCpuTime=$executorCpuTime, input($stageInputBytes, $stageInputRecords), output($stageOutputBytes, $stageOutputRecords), "
      + s"shuffleRead($shuffleReadBytes, $shuffleReadRecords), shuffleWrite($shuffleWriteBytes, $shuffleWriteRecords), tasks=(")
    tasks.foreach(task =>
      builder.append(s"\n\t\t\t\t$task")
    )
    builder.append("))")
    builder.toString()
  }

  override def csvFormat: Seq[String] = {
    val stageSummary = Seq(stageId, attemptId, stageStatus, executorRunTime, executorCpuTime, stageInputBytes, stageInputRecords,
      stageOutputBytes, stageOutputRecords, shuffleReadBytes, shuffleReadRecords, shuffleWriteBytes, shuffleWriteRecords)
      .mkString(separator)
    val tasksSummary = tasks.flatMap(_.csvFormat)
    tasksSummary.map(stageSummary + separator + _)
  }
}

object StageSummary {
  def apply(stage: StageData): StageSummary = {
    val tasks = if (stage.tasks.getOrElse(Map.empty).isEmpty) {
      Seq(TaskSummary.invalid)
    } else {
      stage.tasks.get.values.toSeq.map(TaskSummary.apply)
    }
    new StageSummary(stage.stageId, stage.attemptId, stage.status.toString, stage.executorRunTime, stage.executorCpuTime, stage.inputBytes,
      stage.inputRecords, stage.outputBytes, stage.outputRecords, stage.shuffleReadBytes, stage.shuffleReadRecords, stage.shuffleWriteBytes,
      stage.shuffleWriteRecords, tasks)
  }


  def schema: Seq[String] = {
    Seq("stageId", "attemptId", "stageStatus", "executorRunTime", "executorCpuTime", "stageInputBytes", "stageInputRecords",
      "stageOutputBytes", "stageOutputRecords", "shuffleReadBytes", "shuffleReadRecords", "shuffleWriteBytes", "shuffleWriteRecords")
  }

  def invalid: StageSummary = {
    new StageSummary(-1, -1, "INVALID", -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, Seq(TaskSummary.invalid))
  }
}


class TaskSummary(val taskId: Long,
                  val executorId: String,
                  val totalExecutionTime: Long,
                  val jvmGcTime: Long,
                  val jvmGcTimeProportion: Double,
                  val serializationTime: Long,
                  val serializationTimeProportion: Double,
                  val deserializationTime: Long,
                  val deserializationTimeProportion: Double,
                  val shuffleReadTime: Long,
                  val shuffleReadTimeProportion: Double,
                  val shuffleWriteTime: Long,
                  val shuffleWriteTimeProportion: Double,
                  val taskInputBytes: Long,
                  val taskInputRecords: Long,
                  val taskOutputBytes: Long,
                  val taskOutputRecords: Long
                 ) extends Summary {
  require(totalExecutionTime != 0)

  override def toString: String =
    s"TaskSummary(taskId=$taskId, executorId=$executorId, " +
      s"duration=$totalExecutionTime, " +
      s"gcTime=($jvmGcTime, $jvmGcTimeProportion), " +
      s"serializationTime=($serializationTime, $serializationTimeProportion), " +
      s"deserializationTime=($deserializationTime, $deserializationTimeProportion)" +
      s"shuffleReadTime=($shuffleReadTime, $shuffleReadTimeProportion), " +
      s"shuffleWriteTime=($shuffleWriteTime, $shuffleWriteTimeProportion), " +
      s"input($taskInputBytes, $taskInputRecords)" +
      s"output($taskOutputBytes, $taskOutputRecords)"

  override def csvFormat: Seq[String] = {
    val taskSummary = Seq(taskId, executorId, totalExecutionTime, jvmGcTime, jvmGcTimeProportion, serializationTime,
      serializationTimeProportion, deserializationTime, deserializationTimeProportion, shuffleReadTime, shuffleReadTimeProportion,
      shuffleWriteTime, shuffleWriteTimeProportion, taskInputBytes, taskInputRecords, taskOutputBytes, taskOutputRecords)
      .mkString(separator)
    Seq(taskSummary)
  }
}

object TaskSummary {
  def apply(task: TaskData): TaskSummary = {
    // -1 represent invalid data
    val totalExecutionTime = task.duration.getOrElse(-1L)

    def toProportion(time: Long): Double = (time.toDouble / totalExecutionTime * 100).formatted("%.2f").toDouble

    val metricsOpt = task.taskMetrics
    val jvmGcTime = metricsOpt.map(_.jvmGcTime).getOrElse(0L)
    val jvmGcTimeProportion = toProportion(jvmGcTime)

    val serializationTime = metricsOpt.map(_.resultSerializationTime).getOrElse(0L)
    val serializationTimeProportion = toProportion(serializationTime)
    val deserializationTime = metricsOpt.map(_.executorDeserializeTime).getOrElse(0L)
    val deserializationTimeProportion = toProportion(deserializationTime)

    val shuffleReadTime = metricsOpt.map(_.shuffleReadMetrics.fetchWaitTime).getOrElse(0L)
    val shuffleReadTimeProportion = toProportion(shuffleReadTime)
    val shuffleWriteTime = (metricsOpt.map(_.shuffleWriteMetrics.writeTime).getOrElse(0L) / 1e6).toLong
    val shuffleWriteTimeProportion = toProportion(shuffleWriteTime)

    val inputBytes = metricsOpt.map(_.inputMetrics.bytesRead).getOrElse(0L)
    val inputRecord = metricsOpt.map(_.inputMetrics.recordsRead).getOrElse(0L)
    val outputBytes = metricsOpt.map(_.outputMetrics.bytesWritten).getOrElse(0L)
    val outputRecord = metricsOpt.map(_.outputMetrics.recordsWritten).getOrElse(0L)


    new TaskSummary(task.taskId, task.executorId, totalExecutionTime, jvmGcTime, jvmGcTimeProportion,
      serializationTime, serializationTimeProportion, deserializationTime, deserializationTimeProportion, shuffleReadTime,
      shuffleReadTimeProportion, shuffleWriteTime, shuffleWriteTimeProportion, inputBytes, inputRecord, outputBytes, outputRecord)
  }

  def schema: Seq[String] = {
    Seq("taskId", "executorId", "totalExecutionTime", "jvmGcTime", "jvmGcTimeProportion", "serializationTime",
      "serializationTimeProportion", "deserializationTime", "deserializationTimeProportion", "shuffleReadTime", "shuffleReadTimeProportion",
      "shuffleWriteTime", "shuffleWriteTimeProportion", "taskInputBytes", "taskInputRecords", "taskOutputBytes", "taskOutputRecords")
  }

  def invalid: TaskSummary = {
    new TaskSummary(-1, "invalid", -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1)
  }
}
