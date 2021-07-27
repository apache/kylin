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

package org.apache.spark.deploy

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}

import java.io.{InputStream, StringWriter}

class StandaloneAppClientTest extends SparderBaseFunSuite with LocalMetadata {

  test("Test find state function from HTML and JSON response.") {
    val htmlFile = getClass.getClassLoader.getResourceAsStream("response/application-detail-246.html")
    val jsonFile = getClass.getClassLoader.getResourceAsStream("response/standalone-master-246.json")

    def readF(f: InputStream): String = {
      val writer = new StringWriter()
      IOUtils.copy(f, writer, "UTF-8")
      f.close()
      writer.toString
    }

    val htmlStr = readF(htmlFile)
    val jsonStr = readF(jsonFile)


    StandaloneAppClient.parseApplicationState(jsonStr)
    val res = StandaloneAppClient.getRunningJobs
    val state = StandaloneAppClient.getAppState("6eb0d430-2882-4699-9915-1154959c2cd8")
    assert("FINISHED".equals(state))

    assert(res.size == 3)
    assert(res.contains("app-20210701232026-0001"))
    assert("FINISHED".equals(res.getOrElse("app-20210701232026-0001", ("", "", 1L))._2))
  }
}
