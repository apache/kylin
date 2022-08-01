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

class TestFairSchedulerParser extends SparderBaseFunSuite {

  /**
    * value store in json:
    * cluster, max(185988, 128), used(55296, 45)
    *   root.default, max(185988, 128), used(0 ,0)
    *   root.users.kylin, max(74752, 48), used(0, 0)
    *   root.users.dogfood, max(36864, 24), used(22528, 21)
    *   root.users.root, max(36864, 24), used(32768, 24)
    */

  test("availableResource return correct available resource in target queue") {
    val content = TestUtils.getContent("schedulerInfo/fairSchedulerInfo.json")
    val parser = new FairSchedulerParser
    parser.parse(content)
    val kylinResource = parser.availableResource("root.users.kylin")
    assert(kylinResource == AvailableResource(ResourceInfo(74752, 48), ResourceInfo(74752, 48)))
    val defaultResource = parser.availableResource("root.default")
    assert(defaultResource == AvailableResource(ResourceInfo(130692, 83), ResourceInfo(185988, 128)))
    val dogFoodResource = parser.availableResource("root.users.dogfood")
    assert(dogFoodResource == AvailableResource(ResourceInfo(14336, 3), ResourceInfo(36864, 24)))
    val rootResource = parser.availableResource("root.users.root")
    assert(rootResource == AvailableResource(ResourceInfo(4096, 0), ResourceInfo(36864, 24)))
  }
}
