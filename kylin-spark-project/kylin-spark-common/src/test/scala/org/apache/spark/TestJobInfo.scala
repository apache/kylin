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

class TestJobInfo extends SparderBaseFunSuite {
  private val helper: InfoHelper = new MockInfoHelper
  private val job: JobData = new MockJobData(1, "test", Seq(1), Some("normal"), JobExecutionStatus.SUCCEEDED)
  private val task = new MockTaskData(0, "executor", Some(10), None)
  private val stage: StageData = new MockStageData(StageStatus.COMPLETE, 1, 0, 1, 1, 100,
    10, 50, 5, 20, 2, 10, 1, Some(Map(0L -> task)))

  test("summaries") {
    val queryId = "normal"
    val info = new JobInfo(helper)
    val summaries1 = info.summaries(queryId)
    val expectedJob = Seq(JobSummary.apply(job, Seq(stage).map(StageSummary.apply)))
    assert(summaries1.length == expectedJob.length)
    assert(info.toString == expectedJob.head.toString)
    assert(info.csvFormat == expectedJob.head.csvFormat)
  }

  test("schema") {
    assert(JobInfo.schema == JobSummary.schema ++ StageSummary.schema ++ TaskSummary.schema)
  }

  class MockInfoHelper extends InfoHelper(spark = null) {
    override def getJobsByGroupId(id: String): Seq[JobData] = {
      if (id == "normal") {
        Seq(job)
      } else {
        Seq.empty
      }
    }

    override def getStagesWithDetailsByStageIds(stageIds: Seq[Int]): Seq[StageData] = {
      var stages = Seq.empty[StageData]
      stageIds.foreach {
        case 1 => stages = stages :+ stage
        case _ =>
      }
      stages
    }
  }

}




