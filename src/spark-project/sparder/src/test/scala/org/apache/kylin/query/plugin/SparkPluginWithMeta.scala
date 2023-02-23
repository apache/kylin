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

package org.apache.kylin.query.plugin

import org.apache.kylin.common.util.NLocalFileMetadataTestCase
import org.apache.spark.{SparkContext, SparkFunSuite}
import org.scalatest.BeforeAndAfterAll

import java.io.File

trait SparkPluginWithMeta extends SparkFunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _
  protected val ut_meta = "../examples/test_case_data/localmeta"
  lazy val metaStore: NLocalFileMetadataTestCase = new NLocalFileMetadataTestCase

  protected def metadata: Seq[String] = {
    Seq(fitPathForUT(ut_meta))
  }

  private def fitPathForUT(path: String): String = {
    if (new File(path).exists()) {
      path
    } else {
      s"../$path"
    }
  }

  protected def clearSparkSession(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  override def beforeAll(): Unit = {
    metaStore.createTestMetadata(metadata: _*)
    super.beforeAll()
  }

  protected def overwriteSystemProp(key: String, value: String): Unit = {
    metaStore.overwriteSystemProp(key, value)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    clearSparkSession()
    metaStore.restoreSystemProps()
    metaStore.cleanupTestMetadata()
  }
}