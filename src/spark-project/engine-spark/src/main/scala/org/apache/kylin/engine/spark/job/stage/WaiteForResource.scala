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

package org.apache.kylin.engine.spark.job.stage

import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.commons.lang.StringUtils
import org.apache.spark.application.NoRetryException
import org.apache.spark.utils.ResourceUtils

import java.util.concurrent.TimeUnit

class WaiteForResource(jobContext: SparkApplication) extends StageExec {

  override def getJobContext: SparkApplication = jobContext

  override def getDataSegment: NDataSegment = null

  override def getSegmentId: String = null

  override def getId: String = {
    val jobStepId = StringUtils.replace(buildEnv.buildJobInfos.getJobStepId, SparkApplication.JOB_NAME_PREFIX, "")
    jobStepId + "_00"
  }

  private val config = jobContext.getConfig
  private val buildEnv = KylinBuildEnv.getOrCreate(config)
  private val sparkConf = buildEnv.sparkConf

  override def execute(): Unit = {
    if (jobContext.isJobOnCluster(sparkConf)) {
      val sleepSeconds = (Math.random * 60L).toLong
      logInfo(s"Sleep $sleepSeconds seconds to avoid submitting too many spark job at the same time.")
      KylinBuildEnv.get().buildJobInfos.startWait()
      Thread.sleep(sleepSeconds * 1000)

      // Set check resource timeout limit, otherwise tasks will remain in an endless loop, default is 30 min.
      val checkEnabled: Boolean = config.getCheckResourceEnabled
      var (timeoutLimitNs: Long, startTime: Long, timeTaken: Long) = if (checkEnabled) {
        logInfo(s"CheckResource timeout limit was set: ${config.getCheckResourceTimeLimit} minutes.")
        // can not delete toLong method, may cause match error
        (TimeUnit.NANOSECONDS.convert(config.getCheckResourceTimeLimit, TimeUnit.MINUTES).toLong, System.nanoTime.toLong, 0L)
      } else {
        (-1L, -1L, -1L)
      }
      try while (!ResourceUtils.checkResource(sparkConf, buildEnv.clusterManager, config.isSkipResourceCheck)) {
        if (checkEnabled) {
          timeTaken = System.nanoTime
          if (timeTaken - startTime > timeoutLimitNs) {
            val timeout = TimeUnit.MINUTES.convert(timeTaken - startTime, TimeUnit.NANOSECONDS)
            throw new NoRetryException(s"CheckResource exceed timeout limit: $timeout minutes.")
          }
        }
        val waitTime = (Math.random * 10 * 60).toLong
        logInfo(s"Current available resource in cluster is not sufficient, wait $waitTime seconds.")
        Thread.sleep(waitTime * 1000L)
      } catch {
        case e: NoRetryException =>
          throw e
        case e: Exception =>
          logWarning("Error occurred when check resource. Ignore it and try to submit this job.", e)
      }
      KylinBuildEnv.get().buildJobInfos.endWait()
    }
  }

  override def getStageName: String = "WaiteForResource"
}
