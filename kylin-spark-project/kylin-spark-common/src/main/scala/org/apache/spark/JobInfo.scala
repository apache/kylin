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
