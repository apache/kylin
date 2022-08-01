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

package org.apache.kylin.cluster

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

  test("exec command return non zero ") {
    val stdErr = "2"
    val stdOut = "1"
    val (exitCode, exe_stdout) = SchedulerInfoCmdHelper.execute(s"(>&2 echo $stdErr;echo $stdOut; exit 1)")

    assertResult(1)(exitCode)
    assertResult(stdOut + "\n")(exe_stdout)
  }
}
