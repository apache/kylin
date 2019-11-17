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

package io.kyligence.kap.cluster

import io.kyligence.kap.cluster.parser.SchedulerParserFactory
import io.kyligence.kap.engine.spark.utils.BuildUtils
import org.apache.hadoop.yarn.client.api.YarnClient
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
