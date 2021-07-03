/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.common

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.{LocalFileMetadataTestCase, TempMetadataBuilder}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait LocalMetadata extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestMetadata()
  }


  override def afterAll() {
    super.afterAll()
    try {
      cleanAfterClass()
    } catch {
      case ignore: Exception =>
    }
  }

  def createTestMetadata(): Unit = {
    val tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata
    KylinConfig.setKylinConfigForLocalTest(tempMetadataDir)
  }

  def cleanAfterClass(): Unit = {
    LocalFileMetadataTestCase.cleanAfterClass();
  }
}
