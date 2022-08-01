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
package org.apache.spark.sql

import java.util.TimeZone

import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class SparkTimezoneTest extends SparderBaseFunSuite with SharedSparkSession {

  val defaultTimezone: TimeZone = TimeZone.getDefault

  override def afterAll(): Unit = {
    TimeZone.setDefault(defaultTimezone)
  }

  val schema = StructType(
    Array(
      StructField("id", IntegerType, nullable = true),
      StructField("birth", DateType, nullable = true),
      StructField("time", TimestampType, nullable = true)
    ))

  test("read csv") {
    val path = "./src/test/resources/persisted_df/timezone/csv"
    assertTime(path, "csv", shouldBeEqual = false)
  }

  test("read orc") {
    val path = "./src/test/resources/persisted_df/timezone/orc"
    assertTime(path, "orc", shouldBeEqual = false)
  }

  test("read csv written by spark ") {
    val path = "./src/test/resources/persisted_df/timezone/spark_write_csv"
    assertTime(path, "csv", shouldBeEqual = true)
  }

  test("read parquet") {
    val path = "./src/test/resources/persisted_df/timezone/parquet"
    assertTime(path, "parquet", shouldBeEqual = true)
  }

  def assertTime(path: String, source: String, shouldBeEqual: Boolean): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val ts0 = getTime(spark.read.schema(schema).format(source).load(path))

    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
    val ts1 = getTime(spark.read.schema(schema).format(source).load(path))

    if (shouldBeEqual) {
      assert(ts0 == ts1)
    } else {
      assert(ts0 != ts1)
    }
  }

  private def getTime(df: DataFrame): Long = {
    df.select(col("time")).head().getTimestamp(0).getTime
  }

}
