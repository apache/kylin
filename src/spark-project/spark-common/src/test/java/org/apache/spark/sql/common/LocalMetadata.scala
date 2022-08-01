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
package org.apache.spark.sql.common

import java.io.File
import org.apache.curator.test.TestingServer
import org.apache.kylin.common.util.{NLocalFileMetadataTestCase, Unsafe}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}


trait LocalMetadata extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>
  val metadata = "../examples/test_case_data/localmeta"
  var metaStore: NLocalFileMetadataTestCase = _
  var zkTestServer: TestingServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore = new NLocalFileMetadataTestCase
    if (new File(metadata).exists()) {
      metaStore.createTestMetadata(metadata)
    } else {
      metaStore.createTestMetadata("../" + metadata)
    }
    zkTestServer = new TestingServer(true)
    Unsafe.setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString)
  }

  protected def overwriteSystemProp(key: String, value: String): Unit = {
    metaStore.overwriteSystemProp(key, value)
  }

  override protected def afterEach(): scala.Unit = {
    metaStore.restoreSystemProps()
  }


  override def afterAll() {
    super.afterAll()
    try {
      metaStore.restoreSystemProps()
      metaStore.cleanupTestMetadata()
      if (zkTestServer != null) {
        zkTestServer.close()
        Unsafe.clearProperty("kylin.env.zookeeper-connect-string")
      }
    } catch {
      case ignore: Exception =>
    }
  }
}
