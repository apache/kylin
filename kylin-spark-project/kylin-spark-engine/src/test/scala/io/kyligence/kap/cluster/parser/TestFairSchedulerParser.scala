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

import io.kyligence.kap.cluster.{AvailableResource, ResourceInfo, TestUtils}
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
