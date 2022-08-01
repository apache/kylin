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
import org.apache.spark.sql.catalyst.expressions.TimestampDiff
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{FunctionEntity, Row}
import org.scalatest.BeforeAndAfterAll

class TimestampDiffTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    val function = FunctionEntity(expression[TimestampDiff]("TIMESTAMPDIFF"))
    spark.sessionState.functionRegistry.registerFunction(function.name, function.info, function.builder)
  }

  ignore("test diff between date and date") {
    // YEAR
    verifyResult("select timestampdiff('YEAR', date'2016-02-29' , date'2017-02-28')", Seq("1"))
    verifyResult("select timestampdiff('YEAR', date'2016-02-29' , date'2017-02-27')", Seq("0"))

    // QUARTER
    verifyResult("select timestampdiff('QUARTER', date'2016-01-01' , date'2016-04-01')", Seq("1"))
    verifyResult("select timestampdiff('QUARTER', date'2016-01-01' , date'2016-03-31')", Seq("0"))

    // MONTH
    verifyResult("select timestampdiff('MONTH', date'2016-02-29' , date'2016-03-30')", Seq("1"))
    verifyResult("select timestampdiff('MONTH', date'2016-02-29' , date'2016-01-30')", Seq("-1"))
    verifyResult("select timestampdiff('MONTH', date'2016-02-28' , date'2016-01-30')", Seq("0"))
    verifyResult("select timestampdiff('MONTH', date'2016-02-28' , date'2017-02-28')", Seq("12"))

    // WEEK
    verifyResult("select timestampdiff('WEEK', date'2016-02-01' , date'2016-02-07')", Seq("0"))
    verifyResult("select timestampdiff('WEEK', date'2016-02-01' , date'2016-02-08')", Seq("1"))

    // DAY
    verifyResult("select timestampdiff('DAY', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('DAY', date'2016-02-01' , date'2016-02-02')", Seq("1"))
    // TODO KE-27654
    // verifyResult("select timestampdiff('DAY', date'1977-04-20', date'1987-08-02')", Seq("3756"))

    // HOUR
    verifyResult("select timestampdiff('HOUR', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('HOUR', date'2016-02-01' , date'2016-02-02')", Seq("24"))

    // MINUTE
    verifyResult("select timestampdiff('MINUTE', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('MINUTE', date'2016-02-01' , date'2016-02-02')", Seq("1440"))

    // SECOND
    verifyResult("select timestampdiff('SECOND', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('SECOND', date'2016-02-01' , date'2016-02-02')", Seq("86400"))

    // FRAC_SECOND
    verifyResult("select timestampdiff('FRAC_SECOND', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('FRAC_SECOND', date'2016-02-01' , date'2016-02-02')", Seq("86400000"))
  }

  ignore("test diff between date and timestamp") {
    // YEAR
    verifyResult("select timestampdiff('YEAR', date'2016-02-29' , timestamp'2017-02-28 00:00:00.000')", Seq("1"))
    verifyResult("select timestampdiff('YEAR', date'2016-02-29' , timestamp'2017-02-27 00:00:00.000')", Seq("0"))

    // QUARTER
    verifyResult("select timestampdiff('QUARTER', date'2016-01-01' , timestamp'2016-04-01 00:00:00.000')", Seq("1"))
    verifyResult("select timestampdiff('QUARTER', date'2016-01-01' , timestamp'2016-03-31 00:00:00.000')", Seq("0"))

    // MONTH
    verifyResult("select timestampdiff('MONTH', date'2016-02-29' , timestamp'2016-01-29 00:00:00.000')", Seq("-1"))
    verifyResult("select timestampdiff('MONTH', date'2016-02-29' , timestamp'2016-01-29 00:00:00.001')", Seq("0"))

    // WEEK
    verifyResult("select timestampdiff('WEEK', date'2016-02-08' , timestamp'2016-02-01 00:00:00.000')", Seq("-1"))
    verifyResult("select timestampdiff('WEEK', date'2016-02-08' , timestamp'2016-02-01 00:00:00.001')", Seq("0"))

    // DAY
    verifyResult("select timestampdiff('DAY', date'2016-02-02' , timestamp'2016-02-01 00:00:00.000')", Seq("-1"))
    verifyResult("select timestampdiff('DAY', date'2016-02-02' , timestamp'2016-02-01 00:00:00.001')", Seq("0"))

    // HOUR
    verifyResult("select timestampdiff('HOUR', date'2016-02-01' , timestamp'2016-02-01 01:00:00.000')", Seq("1"))

    // MINUTE
    verifyResult("select timestampdiff('MINUTE', date'2016-02-01' , timestamp'2016-02-01 00:01:00.000')", Seq("1"))

    // SECOND
    verifyResult("select timestampdiff('SECOND', date'2016-02-01' , timestamp'2016-02-01 00:00:01.000')", Seq("1"))

    // FRAC_SECOND
    verifyResult("select timestampdiff('FRAC_SECOND', date'2016-02-01' , timestamp'2016-02-01 00:00:00.001')", Seq("1"))
  }

  ignore("test diff between timestamp and date") {
    // YEAR
    verifyResult("select timestampdiff('YEAR', timestamp'2016-02-29 00:00:00.000' , date'2017-02-28')", Seq("1"))
    verifyResult("select timestampdiff('YEAR', timestamp'2016-02-29 00:00:00.000' , date'2017-02-27')", Seq("0"))

    // QUARTER
    verifyResult("select timestampdiff('QUARTER', timestamp'2016-01-01 00:00:00.000' , date'2016-04-01')", Seq("1"))
    verifyResult("select timestampdiff('QUARTER', timestamp'2016-01-01 00:00:00.000' , date'2016-03-31')", Seq("0"))

    // MONTH
    verifyResult("select timestampdiff('MONTH', timestamp'2016-02-29 00:00:00.000' , date'2016-01-29')", Seq("-1"))

    // WEEK
    verifyResult("select timestampdiff('WEEK', timestamp'2016-02-08 00:00:00.000' , date'2016-02-01')", Seq("-1"))

    // DAY
    verifyResult("select timestampdiff('DAY', timestamp'2016-02-02 00:00:00.000' , date'2016-02-01')", Seq("-1"))

    // HOUR
    verifyResult("select timestampdiff('HOUR', timestamp'2016-02-01 01:00:00.000' , date'2016-02-01')", Seq("-1"))

    // MINUTE
    verifyResult("select timestampdiff('MINUTE', timestamp'2016-02-01 00:01:00.000' , date'2016-02-01')", Seq("-1"))

    // SECOND
    verifyResult("select timestampdiff('SECOND', timestamp'2016-02-01 00:00:01.000' , date'2016-02-01')", Seq("-1"))

    // FRAC_SECOND
    verifyResult("select timestampdiff('FRAC_SECOND', timestamp'2016-02-01 00:00:00.001' , date'2016-02-01')", Seq("-1"))
  }

  ignore("test diff between timestamp and timestamp") {
    // YEAR
    verifyResult("select timestampdiff('YEAR', timestamp'2016-02-29 00:00:00.000' , timestamp'2017-02-28 00:00:00.000')", Seq("1"))

    // QUARTER
    verifyResult("select timestampdiff('QUARTER', timestamp'2016-01-01 00:00:00.000' , timestamp'2016-04-01 00:00:00.000')", Seq("1"))

    // MONTH
    verifyResult("select timestampdiff('MONTH', timestamp'2016-01-29 00:00:00.000' , timestamp'2016-02-29 00:00:00.000')", Seq("1"))

    // WEEK
    verifyResult("select timestampdiff('WEEK', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-08 00:00:00.000')", Seq("1"))

    // DAY
    verifyResult("select timestampdiff('DAY', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-02 00:00:00.000')", Seq("1"))

    // HOUR
    verifyResult("select timestampdiff('HOUR', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-02 01:00:00.000')", Seq("25"))
    verifyResult("select timestampdiff('HOUR', timestamp'2016-02-01 00:00:00.001' , timestamp'2016-02-02 01:00:00.000')", Seq("24"))

    // MINUTE
    verifyResult("select timestampdiff('MINUTE', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-01 01:01:00.000')", Seq("61"))
    verifyResult("select timestampdiff('MINUTE', timestamp'2016-02-01 00:00:00.001' , timestamp'2016-02-01 01:01:00.000')", Seq("60"))

    // SECOND
    verifyResult("select timestampdiff('SECOND', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-01 00:01:01.000')", Seq("61"))
    verifyResult("select timestampdiff('SECOND', timestamp'2016-02-01 00:00:00.001' , timestamp'2016-02-01 00:01:01.000')", Seq("60"))

    // FRAC_SECOND
    verifyResult("select timestampdiff('FRAC_SECOND', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-01 00:00:00.011')", Seq("11"))
  }

  test("test null and illegal argument") {
    verifyResult("select timestampdiff(null, timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-01 00:00:00.011')", Seq("null"))
    verifyResult("select timestampdiff(null, date'2016-02-01' , timestamp'2016-02-01 00:00:00.011')", Seq("null"))
    verifyResult("select timestampdiff(null, timestamp'2016-02-01 00:00:00.000' , date'2016-02-01')", Seq("null"))
    verifyResult("select timestampdiff(null, date'2016-02-01' , date'2016-02-01')", Seq("null"))

    verifyResult("select timestampdiff('DAY', null, timestamp'2016-02-02 00:00:00.011')", Seq("null"))
    verifyResult("select timestampdiff('DAY', null, date'2016-02-01')", Seq("null"))
    verifyResult("select timestampdiff('DAY', timestamp'2016-02-01 00:00:00.000' , null)", Seq("null"))
    verifyResult("select timestampdiff('DAY', date'2016-02-01' , null)", Seq("null"))

    try {
      verifyResult("select timestampdiff('ILLEGAL', date'2016-02-01', date'2016-01-31')", Seq("0"))
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage == "Illegal unit: ILLEGAL, only support [YEAR, SQL_TSI_YEAR, QUARTER, SQL_TSI_QUARTER, MONTH, SQL_TSI_MONTH," +
          " WEEK, SQL_TSI_WEEK, DAY, SQL_TSI_DAY, HOUR, SQL_TSI_HOUR, MINUTE, SQL_TSI_MINUTE, SECOND, SQL_TSI_SECOND," +
          " FRAC_SECOND, SQL_TSI_FRAC_SECOND] for now.")
    }
  }

  test("test codegen") {
    val schema = StructType(List(
      StructField("unit", StringType),
      StructField("timestamp1", TimestampType),
      StructField("timestamp2", TimestampType),
      StructField("date1", DateType),
      StructField("date2", DateType)
    ))
    val rdd = sc.parallelize(Seq(
      Row("MONTH", Timestamp.valueOf("2016-01-31 01:01:01.001"), Timestamp.valueOf("2016-02-29 01:01:01.001"),
        Date.valueOf("2016-01-31"), Date.valueOf("2016-02-29"))
    ))
    spark.sqlContext.createDataFrame(rdd, schema).createOrReplaceGlobalTempView("test_timestamp_diff")
    verifyResult("select timestampdiff(unit, date1, date2) from global_temp.test_timestamp_diff", Seq("1"))
    verifyResult("select timestampdiff(unit, date1, timestamp2) from global_temp.test_timestamp_diff", Seq("1"))
    verifyResult("select timestampdiff(unit, timestamp1, date2) from global_temp.test_timestamp_diff", Seq("0"))
    verifyResult("select timestampdiff(unit, timestamp1, timestamp2) from global_temp.test_timestamp_diff", Seq("1"))
  }

  def verifyResult(sql: String, expect: Seq[String]): Unit = {
    val actual = spark.sql(sql).collect().map(row => row.toString()).mkString(",")
    assert(actual == "[" + expect.mkString(",") + "]")
  }
}
