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

package org.apache.kylin.engine.spark.utils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.common.LocalMetadata
import org.junit.Assert

class StorageUtilsTest extends SparkFunSuite with LocalMetadata {
  test("StorageUtils getCurrentYarnConfigurationFromWriteCluster") {
    System.setProperty("spark.hadoop.123", "123")
    val configuration = StorageUtils.getCurrentYarnConfigurationFromWriteCluster
    val result = configuration.get("123", "0")
    Assert.assertEquals("123", result)
    System.clearProperty("spark.hadoop.123")
  }
}

