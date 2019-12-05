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
