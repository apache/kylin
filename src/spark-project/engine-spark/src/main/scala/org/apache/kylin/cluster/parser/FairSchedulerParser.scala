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

package org.apache.kylin.cluster.parser

import java.util.{List => JList}

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kylin.cluster.{AvailableResource, ResourceInfo}

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
