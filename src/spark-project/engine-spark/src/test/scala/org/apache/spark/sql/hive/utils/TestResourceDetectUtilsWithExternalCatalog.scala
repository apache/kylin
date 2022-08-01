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

package org.apache.spark.sql.hive.utils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.KylinSession.KylinBuilder
import org.apache.spark.sql.execution.datasource.{KylinSourceStrategy, LayoutFileSourceStrategy}
import org.apache.spark.sql.{KylinSession, SparkSession}
import org.apache.spark.sql.newSession.WithKylinExternalCatalog
import org.scalatest.BeforeAndAfterEach

class TestResourceDetectUtilsWithExternalCatalog extends SparkFunSuite with WithKylinExternalCatalog with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    clearSparkSession()
  }

  override def afterEach(): Unit = {
    clearSparkSession()
  }

  test("Test Resource Detect with Moniker External Catalog") {
    val spark = SparkSession.builder
      .master("local")
      .appName("Test Resource Detect with Moniker External Catalog")
      .config("spark.sql.legacy.charVarcharAsString", "true")
      .enableHiveSupport()
      .getOrCreateKylinSession()
    // test partitioned table
    // without filter
    val allSupplier = spark.sql("select * from SSB.SUPPLIER")
    var paths = ResourceDetectUtils.getPaths(allSupplier.queryExecution.sparkPlan)
    assert(3 == paths.size)

    // with one partition filter
    val supplier1 = spark.sql("select * from SSB.SUPPLIER as s where s.S_NATION = 'ETHIOPIA'")
    paths = ResourceDetectUtils.getPaths(supplier1.queryExecution.sparkPlan)
    assert(1 == paths.size)

    // multi level partition
    val supplier2 = spark.sql("select * from SSB.SUPPLIER as s where s.S_NATION = 'PERU' and s.S_CITY='PERU9'")
    paths = ResourceDetectUtils.getPaths(supplier2.queryExecution.sparkPlan)
    assert(1 == paths.size)

    // with one empty partition filter
    val supplier3 = spark.sql("select * from SSB.SUPPLIER as s where s.S_NATION = 'CHINA'")
    paths = ResourceDetectUtils.getPaths(supplier3.queryExecution.sparkPlan)
    assert(0 == paths.size)

    // filter with other col
    val supplier4 = spark.sql("select * from SSB.SUPPLIER as s where s.S_NAME = '2'")
    paths = ResourceDetectUtils.getPaths(supplier4.queryExecution.sparkPlan)
    assert(3 == paths.size)

    // test no partition table
    // test ssb.part
    val allPart = spark.sql("select * from SSB.PART")
    paths = ResourceDetectUtils.getPaths(allPart.queryExecution.sparkPlan)
    assert(1 == paths.size)
  }
}
