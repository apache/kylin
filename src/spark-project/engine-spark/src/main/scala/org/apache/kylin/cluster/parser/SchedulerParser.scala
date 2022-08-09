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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kylin.cluster.AvailableResource
import org.apache.spark.internal.Logging

trait SchedulerParser extends Logging {
  protected var root: JsonNode = _
  protected lazy val mapper = new ObjectMapper

  def availableResource(queueName: String): AvailableResource

  def parse(schedulerInfo: String): Unit = {
    this.root = mapper.readTree(schedulerInfo)
  }

  // value in some scheduler info format to "value"
  protected def parseValue(node: JsonNode): String = {
    if (node.toString.startsWith("\"")) {
      node.toString.replace("\"", "")
    } else {
      node.toString
    }
  }
}
