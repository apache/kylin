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
