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




