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

import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.JobData

class JobInfo(private val helper: InfoHelper) extends Logging {
  private var jobs: Seq[JobData] = _
  private var jobsSummary: Seq[JobSummary] = _

  def summaries(queryId: String): Seq[JobSummary] = {
    jobs = helper.getJobsByGroupId(queryId)
    jobsSummary = jobs.map { job =>
      val stages = getStagesSummary(job.jobId)
      JobSummary.apply(job, stages)
    }
    jobsSummary
  }

  def csvFormat: Seq[String] = jobsSummary.flatMap(_.csvFormat)

  override def toString: String = jobsSummary.map(_.toString).mkString("\n")

  private def getStagesSummary(jobId: Int): Seq[StageSummary] = {
    val job = jobs.filter(_.jobId == jobId).head
    val stages = helper.getStagesWithDetailsByStageIds(job.stageIds)
    stages.map(StageSummary.apply)
  }
}

object JobInfo {
  def schema: Seq[String] = JobSummary.schema() ++ StageSummary.schema ++ TaskSummary.schema
}
