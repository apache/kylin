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

import org.apache.spark.status.api.v1._

class MockJobData(
                   override val jobId: Int,
                   override val name: String,
                   override val stageIds: Seq[Int] = Seq.empty,
                   override val jobGroup: Option[String],
                   override val status: JobExecutionStatus
                 ) extends JobData(jobId, name, None, None, None, stageIds, jobGroup, status, 0, 0,
  0, 0, 0, 0, 0, 0,
  0, 0, 0, Map.empty)

class MockStageData(
                     override val status: StageStatus,
                     override val stageId: Int,
                     override val attemptId: Int,
                     override val executorRunTime: Long,
                     override val executorCpuTime: Long,
                     override val inputBytes: Long,
                     override val inputRecords: Long,
                     override val outputBytes: Long,
                     override val outputRecords: Long,
                     override val shuffleReadBytes: Long,
                     override val shuffleReadRecords: Long,
                     override val shuffleWriteBytes: Long,
                     override val shuffleWriteRecords: Long,
                     override val tasks: Option[Map[Long, TaskData]]
                   ) extends StageData(status, stageId, attemptId, 0, 0, 0, 0,
  0, 0, executorRunTime, executorCpuTime, None, None, None, None, inputBytes, inputRecords, outputBytes,
  outputRecords, shuffleReadBytes, shuffleReadRecords, shuffleWriteBytes, shuffleWriteRecords, 0, 0,
  "", None, "", "", Seq.empty, Seq.empty, tasks, None, Map.empty)

class MockTaskData(
                    override val taskId: Long,
                    override val executorId: String,
                    override val duration: Option[Long],
                    override val taskMetrics: Option[TaskMetrics]
                  ) extends TaskData(taskId, 0, 0, null, None, duration, executorId, "", "",
  "", false, Seq.empty)

class MockTaskMetrics(
                       override val executorDeserializeTime: Long,
                       override val jvmGcTime: Long,
                       override val resultSerializationTime: Long,
                       override val inputMetrics: InputMetrics,
                       override val outputMetrics: OutputMetrics,
                       override val shuffleReadMetrics: ShuffleReadMetrics,
                       override val shuffleWriteMetrics: ShuffleWriteMetrics
                     ) extends TaskMetrics(executorDeserializeTime, 0, 0, 0,
  0, jvmGcTime, resultSerializationTime, 0, 0, 0, inputMetrics,
  outputMetrics, shuffleReadMetrics, shuffleWriteMetrics)

class MockInputMetrics(
                        override val bytesRead: Long,
                        override val recordsRead: Long) extends InputMetrics(bytesRead, recordsRead)

class MockOutputMetrics(
                         override val bytesWritten: Long,
                         override val recordsWritten: Long) extends OutputMetrics(bytesWritten, recordsWritten)


class MockShuffleReadMetrics(override val fetchWaitTime: Long) extends ShuffleReadMetrics(0, 0,
  fetchWaitTime, 0, 0, 0, 0)

class MockShuffleWriteMetrics(override val writeTime: Long) extends ShuffleWriteMetrics(0, writeTime, 0)
