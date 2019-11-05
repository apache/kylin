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

package org.apache.spark.application

import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import org.apache.kylin.common.KylinConfig
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
    Mockito.when(config.getClusterInfoFetcherClassName).thenReturn("io.kyligence.kap.cluster.YarnInfoFetcher")
    KylinBuildEnv.getOrCreate(config)
    val args = Array("-className", "org.apache.spark.application.UnknownThrowableJob")
    val (application, appArgs) = JobWorkSpace.resolveArgs(args)
    val loop = new KylinJobEventLoop
    val monitor = new JobMonitor(loop)
    val worker = new JobWorker(application, appArgs, loop)
    val entry = new JobWorkSpace(loop, monitor, worker)
    assert(entry.run() == 1)
  }
}


