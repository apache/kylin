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

package org.apache.spark.application

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.spark.scheduler.KylinJobEventLoop
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.mockito.Mockito

class TestJobWorkSpace extends SparderBaseFunSuite {
  test("resolve args") {
    val expected = "expected args"
    val args = Array("-className", "org.apache.spark.application.MockSucceedJob", expected)
    val (application, appArgs) = JobWorkSpace.resolveArgs(args)
    assert(application.isInstanceOf[MockSucceedJob])
    assert(appArgs.length == 1)
    assert(appArgs.head == expected)
  }

  test("return 0 when job succeed") {
    val args = Array("-className", "org.apache.spark.application.MockSucceedJob")
    val (application, appArgs) = JobWorkSpace.resolveArgs(args)
    val loop = new KylinJobEventLoop
    val monitor = new JobMonitor(loop)
    val worker = new JobWorker(application, appArgs, loop)
    val entry = new JobWorkSpace(loop, monitor, worker)
    assert(entry.run() == 0)
  }

  test("return 1 when job failed") {
    val config = Mockito.mock(classOf[KylinConfig])
    Mockito.when(config.getClusterManagerClassName).thenReturn("YarnClusterManager")
    KylinBuildEnv.getOrCreate(config)
    val args = Array("-className", "org.apache.spark.application.UnknownThrowableJob")
    val (application, appArgs) = JobWorkSpace.resolveArgs(args)
    val loop = new KylinJobEventLoop
    val monitor = new JobMonitor(loop)
    val worker = new JobWorker(application, appArgs, loop)
    val entry = new JobWorkSpace(loop, monitor, worker)
    assert(entry.run() == 1)
  }

  protected override def afterAll(): Unit = {
    KylinBuildEnv.clean()
  }
}


