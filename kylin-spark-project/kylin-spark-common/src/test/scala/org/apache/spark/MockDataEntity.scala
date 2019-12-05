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
