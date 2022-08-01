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

import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.status.api.v1._

class TestSummary extends SparderBaseFunSuite {
  test("TaskSummary") {
    val inputMetrics = new MockInputMetrics(100, 10)
    val outputMetrics = new MockOutputMetrics(50, 5)
    val shuffleReadMetrics = new MockShuffleReadMetrics(10)
    val shuffleWriteMetrics = new MockShuffleWriteMetrics(5 * 1e6.toLong)
    val taskMetrics = new MockTaskMetrics(10, 2, 10, inputMetrics, outputMetrics, shuffleReadMetrics, shuffleWriteMetrics)
    val taskData = new MockTaskData(0, "executor", Option(50L), Option(taskMetrics))
    val task = TaskSummary.apply(taskData)
    assert(task.csvFormat == Seq(Seq(0, "executor", 50, 2, 4.0, 10, 20.0, 10, 20.0, 10, 20.0, 5, 10.0, 100, 10, 50, 5).mkString("\t")))
    assert(task.toString ==
      s"TaskSummary(taskId=0, executorId=executor, " +
        s"duration=50, " +
        s"gcTime=(2, 4.0), " +
        s"serializationTime=(10, 20.0), " +
        s"deserializationTime=(10, 20.0)" +
        s"shuffleReadTime=(10, 20.0), " +
        s"shuffleWriteTime=(5, 10.0), " +
        s"input(100, 10)" +
        s"output(50, 5)")
    assert(TaskSummary.schema == Seq("taskId", "executorId", "totalExecutionTime", "jvmGcTime", "jvmGcTimeProportion", "serializationTime",
      "serializationTimeProportion", "deserializationTime", "deserializationTimeProportion", "shuffleReadTime", "shuffleReadTimeProportion",
      "shuffleWriteTime", "shuffleWriteTimeProportion", "taskInputBytes", "taskInputRecords", "taskOutputBytes", "taskOutputRecords"))
  }

  test("StageSummary") {
    val stageData = new MockStageData(StageStatus.FAILED, 0, 0, 10, 10, 100, 10, 50, 5, 20, 2, 20, 2, Some(Map.empty))
    val stage = StageSummary.apply(stageData)
    assert(stage.csvFormat == Seq(Seq(0, 0, "FAILED", 10, 10, 100, 10, 50, 5, 20, 2, 20, 2).mkString("\t") +
      "\t" + TaskSummary.invalid.csvFormat.head))
    assert(stage.toString ==
      s"StageSummary(stageId=0, attemptId=0, status=FAILED, executorRunTime=10, " +
        s"executorCpuTime=10, input(100, 10), output(50, 5), "
        + s"shuffleRead(20, 2), shuffleWrite(20, 2), tasks=(\n" +
        s"\t\t\t\t${TaskSummary.invalid.toString}))")
    assert(StageSummary.schema ==
      Seq("stageId", "attemptId", "stageStatus", "executorRunTime", "executorCpuTime", "stageInputBytes", "stageInputRecords",
        "stageOutputBytes", "stageOutputRecords", "shuffleReadBytes", "shuffleReadRecords", "shuffleWriteBytes", "shuffleWriteRecords"))
  }

  test("JobSummary") {
    val jobData = new MockJobData(0, "test", Seq.empty, Some("group"), JobExecutionStatus.FAILED)
    val job = JobSummary.apply(jobData, Seq.empty)
    assert(job.csvFormat == Seq(Seq(0, "test", "group", "FAILED").mkString("\t") + "\t" + StageSummary.invalid.csvFormat.head))
    assert(job.toString ==
      s"\nJobSummary(jobId=0, name=test, jobGroup=group, status=FAILED, stages=(\n" +
        s"\t\t${StageSummary.invalid.toString}))")
    assert(JobSummary.schema == Seq("jobId", "name", "jobGroup", "jobStatus"))
  }
}






