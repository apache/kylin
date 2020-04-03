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

import org.apache.kylin.cluster.{AvailableResource, ResourceInfo, TestUtils}
import org.apache.spark.sql.common.SparderBaseFunSuite

class TestCapacitySchedulerParser extends SparderBaseFunSuite {

  /**
    * value store in json:
    * cluster, max: 100%, used: 13.541667%
    * default, max: 20%, used: 0%, resourceUsed(0, 0)
    * dev_test, max: 50.0%, used: 13.541667%, resourceUsed(19968, 5)
    */

  test("availableResource return correct available resource in target queue") {
    val content = TestUtils.getContent("schedulerInfo/capacitySchedulerInfo.json")
    val parser = new CapacitySchedulerParser
    parser.parse(content)
    val defaultResource = parser.availableResource("default")
    assert(defaultResource == AvailableResource(ResourceInfo(429496729, 429496729), ResourceInfo(429496729, 429496729)))
    val devResource = parser.availableResource("dev_test")
    assert(devResource == AvailableResource(ResourceInfo(53759, 782936739), ResourceInfo(73727, 1073741823)))
  }
}
