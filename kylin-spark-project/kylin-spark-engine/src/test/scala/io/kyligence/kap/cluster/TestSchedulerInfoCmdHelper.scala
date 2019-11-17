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

import org.apache.spark.sql.common.SparderBaseFunSuite

class TestSchedulerInfoCmdHelper extends SparderBaseFunSuite {

  test("getSocketAddress return correct socket address configured in yarn-site.xml") {
    val address = SchedulerInfoCmdHelper.getSocketAddress
    val expectAddress = Map("c70-host74" -> 26001, "c70-host76" -> 26001)
    assert(address == expectAddress)
  }

  test("genCmd return correct cmd") {
    val hostName = "c70-host74"
    val port = 26001
    val targetCmd = s"""curl -k --negotiate -u : "https://$hostName:$port/ws/v1/cluster/scheduler""""
    val cmd = SchedulerInfoCmdHelper.genCmd(hostName, port)
    assert(cmd == targetCmd)
  }

  test("execute return std out when exec command") {
    val stdErr = "2"
    val stdOut = "1"
    val tuple = SchedulerInfoCmdHelper.execute(s"(>&2 echo $stdErr;echo $stdOut)")
    assert(tuple._1 == 0)
    assert(tuple._2 == stdOut + "\n")
  }
}
