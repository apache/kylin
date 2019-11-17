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

import java.util.UUID

import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}

class TestInfoHelper extends SparderBaseFunSuite with SharedSparkSession {
  private var helper: InfoHelper = _
  private val queryId1 = UUID.randomUUID().toString
  private val queryId2 = UUID.randomUUID().toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    helper = new InfoHelper(spark)
    runSparkJob()
  }

  override def afterAll(): Unit = {
    spark.sparkContext.clearJobGroup()
    super.afterAll()
  }

  def runSparkJob(): Unit = {
    spark.sparkContext.setJobGroup(queryId1, "just for test", interruptOnCancel = true)
    val frame1 = spark.range(0, 10).toDF("a")
    frame1.count()
    spark.sparkContext.setJobGroup(queryId2, "just for test", interruptOnCancel = true)
    val frame2 = spark.range(5, 15).toDF("a")
    frame2.join(frame1, "a").count()
  }


  test("getJobsByGroupId") {
    val jobs1 = helper.getJobsByGroupId(queryId1)
    assert(jobs1.length == 1)
    assert(jobs1.head.jobGroup.exists(_.equals(queryId1)))

    val jobs2 = helper.getJobsByGroupId(queryId2)
    assert(jobs2.length == 2)
    assert(jobs2.head.jobGroup.exists(_.equals(queryId2)))
    assert(jobs2.last.jobGroup.exists(_.equals(queryId2)))

    // SparkSession will init before every test file, thus this case worked
    val jobs3 = helper.getJobsByGroupId(null)
    assert(jobs3.length == 3)
  }

  test("getStagesWithDetailsByStageIds") {
    val job1 = helper.getJobsByGroupId(queryId1).head
    val stages = helper.getStagesWithDetailsByStageIds(job1.stageIds)
    // length of stages may greater than 2 when exists failed attempts
    // when this case failed, check weather there are any failed attempts
    assert(stages.length == 2)
    // verify details are loaded
    assert(stages.head.tasks.get.nonEmpty)
    assert(stages.last.tasks.get.nonEmpty)
  }
}
