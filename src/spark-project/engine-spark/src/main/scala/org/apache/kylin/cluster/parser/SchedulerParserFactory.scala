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

import org.apache.kylin.common.util.JsonUtil
import org.apache.spark.internal.Logging

object SchedulerParserFactory extends Logging{
  def create(info: String): SchedulerParser = {
    try {
      val schedulerType = JsonUtil.readValueAsTree(info).findValue("type").toString
      // call contains rather than equals cause of value is surrounded with ", for example: {"type":"fairScheduler"}
      // do not support FifoScheduler for now
      if (schedulerType.contains("capacityScheduler")) {
        new CapacitySchedulerParser
      } else if (schedulerType.contains("fairScheduler")) {
        new FairSchedulerParser
      } else {
        throw new IllegalArgumentException(s"Unsupported scheduler type from scheduler info. $schedulerType")
      }
    } catch {
      case throwable: Throwable =>
        logError(s"Invalid scheduler info. $info")
        throw throwable
    }
  }
}
