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

package org.apache.kylin.engine.spark.job

import org.apache.kylin.engine.spark.scheduler.JobRuntime
import org.apache.kylin.metadata.model.NDataModel
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.SparkSession
import org.apache.spark.tracker.BuildContext
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit

class TestSegmentExec extends AnyFunSuite with PrivateMethodTester {

  private val myexec = new SegmentExec {
    override protected val jobId: String = ""
    override protected val project: String = ""
    override protected val segmentId: String = ""
    override protected val dataflowId: String = ""
    override protected val config: KylinConfig = null
    override protected val sparkSession: SparkSession = null
    override protected val dataModel: NDataModel = null
    override protected val storageType: Int = 0
    override protected val resourceContext: BuildContext = null
    override protected val runtime: JobRuntime = null

    override protected def columnIdFunc(colRef: TblColRef): String = ""

    override protected val sparkSchedulerPool: String = ""
  }

  test("test handle failure") {
    val func1 = PrivateMethod[Unit]('handleFailure)
    // test null
    val param1 = null
    myexec invokePrivate func1(param1)

    // test none
    val param2 = None
    myexec invokePrivate func1(param2)

    // test failure
    val param3 = Some(new Exception("test failure 1"))
    assertThrows[Exception](myexec invokePrivate func1(param3))
  }

  test("test fail fast poll") {
    val func1 = PrivateMethod[Int]('failFastPoll)
    // test illegal argument
    assertThrows[AssertionError](myexec invokePrivate func1(0L, TimeUnit.SECONDS))
  }
}
