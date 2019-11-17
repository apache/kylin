/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
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






