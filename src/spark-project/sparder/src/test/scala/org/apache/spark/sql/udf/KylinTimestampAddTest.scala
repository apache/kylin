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

import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.KylinTimestampAdd
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{FunctionEntity, Row}
import org.scalatest.BeforeAndAfterAll

import java.sql.{Date, Timestamp}

class KylinTimestampAddTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()

    val function = FunctionEntity(expression[KylinTimestampAdd]("TIMESTAMPADD"))
    spark.sessionState.functionRegistry.registerFunction(function.name, function.info, function.builder)
  }

  test("test add on date") {
    // YEAR
    verifyResult("select timestampadd('YEAR', 1 , date'2016-02-29')", Seq("2017-02-28"))

    verifyResult("select timestampadd('SQL_TSI_YEAR', 1 , date'2016-02-29')", Seq("2017-02-28"))

    verifyResult("select timestampadd('YEAR', 1 , '2016-02-29')", Seq("2017-02-28 00:00:00.0"))

    verifyResult("select timestampadd('SQL_TSI_YEAR', 1 , '2016-02-29')", Seq("2017-02-28 00:00:00.0"))

    // QUARTER
    verifyResult("select timestampadd('QUARTER', 1L , date'2016-02-29')", Seq("2016-05-31"))

    verifyResult("select timestampadd('SQL_TSI_QUARTER', 1L , date'2016-02-29')", Seq("2016-05-31"))

    // MONTH
    verifyResult("select timestampadd('MONTH', 1 , date'2016-01-31')", Seq("2016-02-29"))

    verifyResult("select timestampadd('SQL_TSI_MONTH', 1 , date'2016-01-31')", Seq("2016-02-29"))

    verifyResult("select timestampadd('MONTH', 1 , '2016-01-31')", Seq("2016-02-29 00:00:00.0"))

    verifyResult("select timestampadd('SQL_TSI_MONTH', 1 , '2016-01-31')", Seq("2016-02-29 00:00:00.0"))

    // WEEK
    verifyResult("select timestampadd('WEEK', 1L , date'2016-01-31')", Seq("2016-02-07"))

    verifyResult("select timestampadd('SQL_TSI_WEEK', 1L , date'2016-01-31')", Seq("2016-02-07"))

    verifyResult("select timestampadd('WEEK', 1L , '2016-01-31')", Seq("2016-02-07 00:00:00.0"))

    verifyResult("select timestampadd('SQL_TSI_WEEK', 1L , '2016-01-31')", Seq("2016-02-07 00:00:00.0"))

    // DAY
    verifyResult("select timestampadd('DAY', 1 , date'2016-01-31')", Seq("2016-02-01"))

    verifyResult("select timestampadd('SQL_TSI_DAY', 1 , date'2016-01-31')", Seq("2016-02-01"))

    verifyResult("select timestampadd('DAY', 1 , '2016-01-31')", Seq("2016-02-01 00:00:00.0"))

    verifyResult("select timestampadd('SQL_TSI_DAY', 1 , '2016-01-31')", Seq("2016-02-01 00:00:00.0"))

    // HOUR
    verifyResult("select timestampadd('HOUR', 1L , date'2016-01-31')", Seq("2016-01-31 01:00:00.0"))

    verifyResult("select timestampadd('SQL_TSI_HOUR', 1L , date'2016-01-31')", Seq("2016-01-31 01:00:00.0"))

    verifyResult("select timestampadd('HOUR', 1L , '2016-01-31')", Seq("2016-01-31 01:00:00.0"))

    verifyResult("select timestampadd('SQL_TSI_HOUR', 1L , '2016-01-31')", Seq("2016-01-31 01:00:00.0"))

    // MINUTE
    verifyResult("select timestampadd('MINUTE', 1 , date'2016-01-31')", Seq("2016-01-31 00:01:00.0"))

    verifyResult("select timestampadd('SQL_TSI_MINUTE', 1 , date'2016-01-31')", Seq("2016-01-31 00:01:00.0"))

    verifyResult("select timestampadd('MINUTE', 1 , '2016-01-31')", Seq("2016-01-31 00:01:00.0"))

    verifyResult("select timestampadd('SQL_TSI_MINUTE', 1 , '2016-01-31')", Seq("2016-01-31 00:01:00.0"))

    // SECOND
    verifyResult("select timestampadd('SECOND', 1L , date'2016-01-31')", Seq("2016-01-31 00:00:01.0"))

    verifyResult("select timestampadd('SQL_TSI_SECOND', 1L , date'2016-01-31')", Seq("2016-01-31 00:00:01.0"))

    verifyResult("select timestampadd('SECOND', 1L , '2016-01-31')", Seq("2016-01-31 00:00:01.0"))

    verifyResult("select timestampadd('SQL_TSI_SECOND', 1L , '2016-01-31')", Seq("2016-01-31 00:00:01.0"))

    // FRAC_SECOND
    verifyResult("select timestampadd('FRAC_SECOND', 1 , date'2016-01-31')", Seq("2016-01-31 00:00:00.001"))

    verifyResult("select timestampadd('SQL_TSI_FRAC_SECOND', 1 , date'2016-01-31')", Seq("2016-01-31 00:00:00.001"))

    verifyResult("select timestampadd('FRAC_SECOND', 1 , '2016-01-31')", Seq("2016-01-31 00:00:00.001"))

    verifyResult("select timestampadd('SQL_TSI_FRAC_SECOND', 1 , '2016-01-31')", Seq("2016-01-31 00:00:00.001"))
  }

  test("test add on timestamp") {
    // YEAR
    verifyResult("select timestampadd('YEAR', 1 , timestamp'2016-02-29 01:01:01.001')", Seq("2017-02-28 01:01:01.001"))
    verifyResult("select timestampadd('YEAR', 1 , '2016-02-29 01:01:01.001')", Seq("2017-02-28 01:01:01.001"))

    // QUARTER
    // verifyResult("select timestampadd('QUARTER', 1L , timestamp'2016-02-29 01:01:01.001')", Seq("2016-05-31 01:01:01.001"))

    // MONTH
    verifyResult("select timestampadd('MONTH', 1 , timestamp'2016-01-31 01:01:01.001')", Seq("2016-02-29 01:01:01.001"))
    verifyResult("select timestampadd('MONTH', 1 , '2016-01-31 01:01:01.001')", Seq("2016-02-29 01:01:01.001"))

    // WEEK
    verifyResult("select timestampadd('WEEK', 1L , timestamp'2016-01-31 01:01:01.001')", Seq("2016-02-07 01:01:01.001"))
    verifyResult("select timestampadd('WEEK', 1L , '2016-01-31 01:01:01.001')", Seq("2016-02-07 01:01:01.001"))

    // DAY
    verifyResult("select timestampadd('DAY', 1 , timestamp'2016-01-31 01:01:01.001')", Seq("2016-02-01 01:01:01.001"))
    verifyResult("select timestampadd('DAY', 1 , '2016-01-31 01:01:01.001')", Seq("2016-02-01 01:01:01.001"))

    // HOUR
    verifyResult("select timestampadd('HOUR', 25L , timestamp'2016-01-31 01:01:01.001')", Seq("2016-02-01 02:01:01.001"))
    verifyResult("select timestampadd('HOUR', 25L , '2016-01-31 01:01:01.001')", Seq("2016-02-01 02:01:01.001"))

    // MINUTE
    verifyResult("select timestampadd('MINUTE', 61 , timestamp'2016-01-31 01:01:01.001')", Seq("2016-01-31 02:02:01.001"))
    verifyResult("select timestampadd('MINUTE', 61 , '2016-01-31 01:01:01.001')", Seq("2016-01-31 02:02:01.001"))

    // SECOND
    verifyResult("select timestampadd('SECOND', 61L , timestamp'2016-01-31 01:01:01.001')", Seq("2016-01-31 01:02:02.001"))
    verifyResult("select timestampadd('SECOND', 61L , '2016-01-31 01:01:01.001')", Seq("2016-01-31 01:02:02.001"))

    // FRAC_SECOND
    verifyResult("select timestampadd('FRAC_SECOND', 1001 , timestamp'2016-01-31 01:01:01.001')", Seq("2016-01-31 01:01:02.002"))
    verifyResult("select timestampadd('FRAC_SECOND', 1001 , '2016-01-31 01:01:01.001')", Seq("2016-01-31 01:01:02.002"))
  }

  test("test null and illegal argument") {
    verifyResult("select timestampadd(null, 1 , timestamp'2016-01-31 01:01:01.001')", Seq("null"))
    verifyResult("select timestampadd(null, 1L , date'2016-01-31')", Seq("null"))
    verifyResult("select timestampadd(null, 1 , '2016-01-31 01:01:01.001')", Seq("null"))
    verifyResult("select timestampadd(null, 1L , '2016-01-31')", Seq("null"))

    verifyResult("select timestampadd('DAY', null , timestamp'2016-01-31 01:01:01.001')", Seq("null"))
    verifyResult("select timestampadd('DAY', null , date'2016-01-31')", Seq("null"))
    verifyResult("select timestampadd('DAY', null , '2016-01-31 01:01:01.001')", Seq("null"))
    verifyResult("select timestampadd('DAY', null , '2016-01-31')", Seq("null"))

    verifyResult("select timestampadd('DAY', 1 , null)", Seq("null"))

    try {
      verifyResult("select timestampadd('ILLEGAL', 1 , date'2016-01-31')", Seq("null"))
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage == "Illegal unit: ILLEGAL, only support [YEAR, SQL_TSI_YEAR, QUARTER, SQL_TSI_QUARTER, MONTH, SQL_TSI_MONTH," +
          " WEEK, SQL_TSI_WEEK, DAY, SQL_TSI_DAY, HOUR, SQL_TSI_HOUR, MINUTE, SQL_TSI_MINUTE, SECOND, SQL_TSI_SECOND," +
          " FRAC_SECOND, SQL_TSI_FRAC_SECOND] for now.")
    }

    try {
      verifyResult("select timestampadd('ILLEGAL', 1 , '2016-01-31')", Seq("null"))
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage == "Illegal unit: ILLEGAL, only support [YEAR, SQL_TSI_YEAR, QUARTER, SQL_TSI_QUARTER, MONTH, SQL_TSI_MONTH," +
          " WEEK, SQL_TSI_WEEK, DAY, SQL_TSI_DAY, HOUR, SQL_TSI_HOUR, MINUTE, SQL_TSI_MINUTE, SECOND, SQL_TSI_SECOND," +
          " FRAC_SECOND, SQL_TSI_FRAC_SECOND] for now.")
    }

    try {
      verifyResult("select timestampadd('ILLEGAL', 2147483648, date'2016-01-31')", Seq("0"))
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage == "Increment(2147483648) is greater than Int.MaxValue")
    }

    try {
      verifyResult("select timestampadd('ILLEGAL', 2147483648, '2016-01-31')", Seq("0"))
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage == "Increment(2147483648) is greater than Int.MaxValue")
    }
  }

  test("test codegen") {
    val schema = StructType(List(
      StructField("c_long", LongType),
      StructField("c_int", IntegerType),
      StructField("unit", StringType),
      StructField("c_timestamp", TimestampType),
      StructField("c_date", DateType),
      StructField("c_timestamp_string", StringType),
      StructField("c_date_string", StringType)
    ))
    val rdd = sc.parallelize(Seq(
      Row(1L, 2, "YEAR", Timestamp.valueOf("2016-02-29 01:01:01.001"), Date.valueOf("2016-02-29"), "2016-02-29 01:01:01.001", "2016-02-29")
    ))
    spark.sqlContext.createDataFrame(rdd, schema).createOrReplaceGlobalTempView("test_timestamp_add")
    verifyResult("select timestampadd(unit, c_long, c_timestamp) from global_temp.test_timestamp_add", Seq("2017-02-28 01:01:01.001"))
    verifyResult("select timestampadd(unit, c_int, c_date) from global_temp.test_timestamp_add", Seq("2018-02-28"))
    verifyResult("select timestampadd(unit, c_long, c_timestamp_string) from global_temp.test_timestamp_add",
      Seq("2017-02-28 01:01:01.001"))
    verifyResult("select timestampadd(unit, c_int, c_date_string) from global_temp.test_timestamp_add", Seq("2018-02-28 00:00:00.0"))
  }

  def verifyResult(sql: String, expect: Seq[String]): Unit = {
    val actual = spark.sql(sql).collect().map(row => row.toString()).mkString(",")
    assert(actual == "[" + expect.mkString(",") + "]")
  }
}
