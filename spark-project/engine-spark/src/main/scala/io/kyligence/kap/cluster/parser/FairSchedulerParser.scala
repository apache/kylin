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

package io.kyligence.kap.cluster.parser

import java.util.{List => JList}

import com.fasterxml.jackson.databind.JsonNode
import io.kyligence.kap.cluster.{AvailableResource, ResourceInfo}

import scala.collection.JavaConverters._

class FairSchedulerParser extends SchedulerParser {

  override def availableResource(queueName: String): AvailableResource = {
    val cluster = clusterAvailableResource()
    val queue = queueAvailableResource(queueName)
    val resource = AvailableResource(cluster.available.reduceMin(queue.available), queue.max)
    logInfo(s"Current actual available resource: $resource.")
    resource
  }

  private def queueAvailableResource(queueName: String): AvailableResource = {
    val queues: JList[JsonNode] = root.findParents("queueName")
    val nodes = queues.asScala.filter(queue => parseValue(queue.get("queueName")).equals(queueName))
    require(nodes.size == 1)
    val resource = calAvailableResource(nodes.head)
    logInfo(s"Queue available resource: $resource.")
    resource
  }

  private def clusterAvailableResource(): AvailableResource = {
    val node = root.findValue("rootQueue")
    require(node != null)
    val resource = calAvailableResource(node)
    logInfo(s"Cluster available resource: $resource.")
    resource
  }

  private def calAvailableResource(node: JsonNode): AvailableResource = {
    val maxResources = node.get("maxResources")
    val maxMemory = parseValue(maxResources.get("memory")).toInt
    val maxCores = parseValue(maxResources.get("vCores")).toInt
    val usedResources = node.get("usedResources")
    val usedMemory = parseValue(usedResources.get("memory")).toInt
    val usedCores = parseValue(usedResources.get("vCores")).toInt
    AvailableResource(ResourceInfo(maxMemory - usedMemory, maxCores - usedCores), ResourceInfo(maxMemory, maxCores))
  }
}
