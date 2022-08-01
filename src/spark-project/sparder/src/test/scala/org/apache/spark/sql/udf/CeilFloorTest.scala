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

import org.apache.spark.sql.FunctionEntity
import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.{CeilDateTime, FloorDateTime}
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
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
    query("select ceil_datetime(date'2012-02-29', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'hour')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'minute')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'second')").equals("[2012-02-29 00:00:00.0]")

    query("select ceil_datetime('2012-02-29', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'hour')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'minute')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'second')").equals("[2012-02-29 00:00:00.0]")

    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'DAY')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'hour')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'minute')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'second')").equals("[2012-03-01 00:00:00.0]")

    query("select ceil_datetime('2012-02-29 23:59:59.1', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'DAY')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'hour')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'minute')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'second')").equals("[2012-03-01 00:00:00.0]")
  }

  test("test floor") {
    query("select floor_datetime(date'2012-02-29', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'hour')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'minute')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'second')").equals("[2012-02-29 00:00:00.0]")

    query("select floor_datetime('2012-02-29', 'year')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'quarter')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'month')").equals("[2012-02-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'week')").equals("[2012-02-27 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'hour')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'minute')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'second')").equals("[2012-02-29 00:00:00.0]")

    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'year')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'quarter')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'month')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'week')").equals("[2012-02-27 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'hour')").equals("[2012-02-29 23:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'minute')").equals("[2012-02-29 23:59:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'second')").equals("[2012-02-29 23:59:59.0]")

    query("select floor_datetime('2012-02-29 23:59:59.1', 'year')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'quarter')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'month')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'week')").equals("[2012-02-27 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'hour')").equals("[2012-02-29 23:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'minute')").equals("[2012-02-29 23:59:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'second')").equals("[2012-02-29 23:59:59.0]")
  }

  test("test spark ceil/floor") {
    query("select floor(-3.12)").equals("[3]")
  }

  def query(sql: String): String = {
    spark.sql(sql).collect().map(row => row.toString()).mkString
  }
}
