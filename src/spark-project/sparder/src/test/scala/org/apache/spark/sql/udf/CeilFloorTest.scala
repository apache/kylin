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

package org.apache.spark.sql.udf

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.{CeilDateTime, FloorDateTime}
import org.apache.spark.sql.common.{GlutenTestUtil, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.types._
import org.apache.spark.sql.{FunctionEntity, Row}
import org.scalatest.BeforeAndAfterAll

class CeilFloorTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()

    val ceil = FunctionEntity(expression[CeilDateTime]("ceil_datetime"))
    val floor = FunctionEntity(expression[FloorDateTime]("floor_datetime"))
    spark.sessionState.functionRegistry.registerFunction(ceil.name, ceil.info, ceil.builder)
    spark.sessionState.functionRegistry.registerFunction(floor.name, floor.info, floor.builder)
  }

  test("test ceil") {
    query("select ceil_datetime(date'2012-02-29', 'year')", "[2013-01-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'quarter')", "[2012-04-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'month')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'week')", "[2012-03-05 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'DAY')", "[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'hour')", "[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'minute')", "[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'second')", "[2012-02-29 00:00:00.0]")

    query("select ceil_datetime('2012-02-29', 'year')", "[2013-01-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'quarter')", "[2012-04-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'month')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'week')", "[2012-03-05 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'DAY')", "[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'hour')", "[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'minute')", "[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'second')", "[2012-02-29 00:00:00.0]")

    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'year')", "[2013-01-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'quarter')", "[2012-04-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'month')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'week')", "[2012-03-05 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'DAY')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'hour')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'minute')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'second')", "[2012-03-01 00:00:00.0]")

    query("select ceil_datetime('2012-02-29 23:59:59.1', 'year')", "[2013-01-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'quarter')", "[2012-04-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'month')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'week')", "[2012-03-05 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'DAY')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'hour')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'minute')", "[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'second')", "[2012-03-01 00:00:00.0]")
  }

  test("test floor") {
    query("select floor_datetime(date'2012-02-29', 'year')", "[2012-01-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'quarter')", "[2012-01-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'month')", "[2012-02-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'week')", "[2012-02-27 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'DAY')", "[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'hour')", "[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'minute')", "[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'second')", "[2012-02-29 00:00:00.0]")

    query("select floor_datetime('2012-02-29', 'year')", "[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'quarter')", "[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'month')", "[2012-02-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'week')", "[2012-02-27 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'DAY')", "[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'hour')", "[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'minute')", "[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'second')", "[2012-02-29 00:00:00.0]")

    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'year')", "[2012-01-01 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'quarter')", "[2012-01-01 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'month')", "[2012-02-01 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'week')", "[2012-02-27 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'DAY')", "[2012-02-29 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'hour')", "[2012-02-29 23:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'minute')", "[2012-02-29 23:59:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'second')", "[2012-02-29 23:59:59.0]")

    query("select floor_datetime('2012-02-29 23:59:59.1', 'year')", "[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'quarter')", "[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'month')", "[2012-02-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'week')", "[2012-02-27 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'DAY')", "[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'hour')", "[2012-02-29 23:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'minute')", "[2012-02-29 23:59:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'second')", "[2012-02-29 23:59:59.0]")
  }

  test("test spark ceil/floor") {
    query("select floor(-3.12)", "[-4]")
  }

  test("test temp dataset") {
    val schema = StructType(List(
      StructField("row_id", LongType),
      StructField("date_col", DateType),
      StructField("timestamp_col", TimestampType)
    ))
    val rdd = sc.parallelize(Seq(
      Row(1L, Date.valueOf("2024-01-01"), Timestamp.valueOf("2024-01-01 01:01:01.001")),
      Row(2L, Date.valueOf("2024-02-29"), Timestamp.valueOf("2024-02-29 02:00:00.000")),
      Row(3L, Date.valueOf("2024-03-31"), Timestamp.valueOf("2024-03-31 00:00:00.000")),
      Row(4L, Date.valueOf("2024-04-01"), Timestamp.valueOf("2024-04-01 00:00:00.001")),
      Row(5L, Date.valueOf("2024-04-30"), Timestamp.valueOf("2024-04-30 01:01:01.001"))
    ))
    spark.sqlContext.createDataFrame(rdd, schema).createTempView("temp_data")
    query("select floor_datetime(date_col, 'year') from temp_data where row_id = 1", "[2024-01-01 00:00:00.0]", true)
    query("select ceil_datetime(date_col, 'year') from temp_data where row_id = 1", "[2024-01-01 00:00:00.0]", true)
    query("select ceil_datetime(timestamp_col, 'year') from temp_data where row_id = 1", "[2025-01-01 00:00:00.0]", true)
    query("select floor_datetime(timestamp_col, 'quarter') from temp_data where row_id = 4", "[2024-04-01 00:00:00.0]", true)
    query("select ceil_datetime(date_col, 'quarter') from temp_data where row_id = 4", "[2024-04-01 00:00:00.0]", true)
    query("select ceil_datetime(timestamp_col, 'quarter') from temp_data where row_id = 4", "[2024-07-01 00:00:00.0]", true)
    query("select floor_datetime(timestamp_col, 'month') from temp_data where row_id = 5", "[2024-04-01 00:00:00.0]", true)
    query("select ceil_datetime(date_col, 'month') from temp_data where row_id = 5", "[2024-05-01 00:00:00.0]", true)
    query("select ceil_datetime(timestamp_col, 'month') from temp_data where row_id = 5", "[2024-05-01 00:00:00.0]", true)
    query("select floor_datetime(timestamp_col, 'week') from temp_data where row_id = 5", "[2024-04-29 00:00:00.0]", true)
    query("select ceil_datetime(date_col, 'week') from temp_data where row_id = 5", "[2024-05-06 00:00:00.0]", true)
    query("select ceil_datetime(timestamp_col, 'week') from temp_data where row_id = 5", "[2024-05-06 00:00:00.0]", true)
    query("select floor_datetime(timestamp_col, 'day') from temp_data where row_id = 3", "[2024-03-31 00:00:00.0]", true)
    query("select ceil_datetime(date_col, 'day') from temp_data where row_id = 3", "[2024-03-31 00:00:00.0]", true)
    query("select ceil_datetime(timestamp_col, 'day') from temp_data where row_id = 3", "[2024-03-31 00:00:00.0]", true)
    query("select floor_datetime(timestamp_col, 'hour') from temp_data where row_id = 5", "[2024-04-30 01:00:00.0]", true)
    query("select ceil_datetime(timestamp_col, 'hour') from temp_data where row_id = 5", "[2024-04-30 02:00:00.0]", true)
    query("select floor_datetime(timestamp_col, 'minute') from temp_data where row_id = 5", "[2024-04-30 01:01:00.0]", true)
    query("select ceil_datetime(timestamp_col, 'minute') from temp_data where row_id = 5", "[2024-04-30 01:02:00.0]", true)
    query("select floor_datetime(timestamp_col, 'second') from temp_data where row_id = 5", "[2024-04-30 01:01:01.0]", true)
    query("select ceil_datetime(timestamp_col, 'second') from temp_data where row_id = 5", "[2024-04-30 01:01:02.0]", true)
  }

  def query(sql: String, expected: String, fallBackCheck: Boolean = false): Unit = {
    val df = spark.sql(sql)
    if (fallBackCheck && GlutenTestUtil.glutenEnabled(spark)) {
      assert(!GlutenTestUtil.hasFallbackOnStep(df.queryExecution.executedPlan, classOf[ProjectExec]))
    }
    val result = df.collect().map(row => row.toString()).mkString
    if (!result.equals(expected)) {
      print(sql)
      print(result)
    }
    assert(result.equals(expected))
  }
}
