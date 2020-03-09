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

import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.kylin.cluster.parser.SchedulerParserFactory
import org.apache.kylin.engine.spark.utils.BuildUtils
import org.apache.spark.internal.Logging

class YarnInfoFetcher extends ClusterInfoFetcher with Logging {
  private lazy val maximumResourceAllocation = {
    val yarnClient = YarnClient.createYarnClient
    var resourceInfo: ResourceInfo = null
    try {
      yarnClient.init(BuildUtils.getCurrentYarnConfiguration)
      yarnClient.start()
      val response = yarnClient.createApplication().getNewApplicationResponse
      resourceInfo = new ResourceInfo(response.getMaximumResourceCapability)
      logInfo(s"Cluster maximum resource allocation $resourceInfo")
    } catch {
      case throwable: Throwable =>
        logError("Error occurred when get resource upper limit per container.", throwable)
        throw throwable
    } finally {
      yarnClient.stop()
    }
    resourceInfo
  }

  override def fetchMaximumResourceAllocation: ResourceInfo = maximumResourceAllocation

  override def fetchQueueAvailableResource(queueName: String): AvailableResource = {
    val info = SchedulerInfoCmdHelper.schedulerInfo
    val parser = SchedulerParserFactory.create(info)
    parser.parse(info)
    parser.availableResource(queueName)
  }
}
