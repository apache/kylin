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

package org.apache.kylin.query.plugin.diagnose

import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv

import java.util.concurrent.{CountDownLatch, TimeUnit}

object DiagnoseHelper extends Logging {
  var state: String = DiagnoseConstant.STATE_WAIT
  var resultCollectionTimeout: Long = KylinConfig.getInstanceFromEnv.queryDiagnoseCollectionTimeout
  var gcResult: CountDownLatch = _

  // activeExecutorCount will be set for UT
  var activeExecutorCount: Int = 0

  def collectSparderExecutorGc(): Unit = {
    if (!KylinConfig.getInstanceFromEnv.isUTEnv) {
      activeExecutorCount = SparderEnv.getActiveExecutorIds.size
    }
    initGcCount(activeExecutorCount)

    setState(DiagnoseConstant.STATE_COLLECT)

    if (gcResult.await(resultCollectionTimeout, TimeUnit.MILLISECONDS)) {
      logInfo("All executor gc logs have been uploaded to hdfs")
    } else {
      logWarning("Timeout while waiting for gc log result")
    }
    setState(DiagnoseConstant.STATE_WAIT)
  }

  def initGcCount(count: Int): Unit = {
    gcResult = new CountDownLatch(count)
  }

  def setState(_state: String): Unit = {
    DiagnoseHelper.state = _state
  }

  def countDownGcResult(): Unit = {
    gcResult.countDown()
  }
}
