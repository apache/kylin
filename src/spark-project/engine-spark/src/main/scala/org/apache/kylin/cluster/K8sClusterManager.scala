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
package org.apache.kylin.cluster

import java.util

import com.google.common.collect.Lists
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class K8sClusterManager extends IClusterManager with Logging {
  override def fetchMaximumResourceAllocation: ResourceInfo = {
    ResourceInfo(Int.MaxValue, 1000)
  }

  override def fetchQueueAvailableResource(queueName: String): AvailableResource = {
    AvailableResource(ResourceInfo(Int.MaxValue, 1000), ResourceInfo(Int.MaxValue, 1000))
  }

  override def getBuildTrackingUrl(sparkSession: SparkSession): String = {
    logInfo("Get Build Tracking Url!")
    val applicationId = sparkSession.sparkContext.applicationId
    ""
  }

  override def killApplication(jobStepId: String): Unit = {
    logInfo("Kill Application $jobStepId !")
  }

  override def killApplication(jobStepPrefix: String, jobStepId: String): Unit = {
    logInfo("Kill Application $jobStepPrefix $jobStepId !")
  }

  override def isApplicationBeenKilled(applicationId: String): Boolean = {
    true
  }

  override def getRunningJobs(queues: util.Set[String]): util.List[String] = {
    Lists.newArrayList()
  }

  override def fetchQueueStatistics(queueName: String): ResourceInfo = {
    ResourceInfo(Int.MaxValue, 1000)
  }

  override def applicationExisted(jobId: String): Boolean = {
    false
  }

}
