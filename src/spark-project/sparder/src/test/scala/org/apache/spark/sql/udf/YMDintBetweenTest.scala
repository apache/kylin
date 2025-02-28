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

import java.sql.Date

import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.YMDintBetween
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{FunctionEntity, Row}
import org.scalatest.BeforeAndAfterAll

class YMDintBetweenTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val function = FunctionEntity(expression[YMDintBetween]("_ymdint_between"))
    spark.sessionState.functionRegistry.registerFunction(function.name, function.info, function.builder)
  }

  test("test _ymdint_between") {
    verifyResult("select _YMDINT_BETWEEN(date'1990-04-30' , date'2003-02-05')", Seq("120906"))
    verifyResult("select _ymdint_between('1990-04-30' , date'2003-02-05')", Seq("120906"))
    verifyResult("select _ymdint_between(date'1990-04-30' , '2003-02-05')", Seq("120906"))
    verifyResult("select _ymdint_between('1990-04-30' , '2003-02-05')", Seq("120906"))
    // Testing for leap years
    verifyResult("select _ymdint_between('2020-01-01' , '2021-01-01')", Seq("10000"))
    // Testing average year
    verifyResult("select _ymdint_between('2021-01-01' , '2022-01-01')", Seq("10000"))


    verifyResult("select _ymdint_between(timestamp'2016-02-01 08:00:00.000' , timestamp'2016-02-01 00:00:00.011')", Seq("00000"))
    verifyResult("select _ymdint_between(date'2016-02-01' , timestamp'2016-02-01 01:00:00.011')", Seq("00000"))
    verifyResult("select _ymdint_between(timestamp'2016-01-01 00:00:00.000' , date'2016-02-01')", Seq("00100"))

    verifyResult("select _ymdint_between('2016/02/01', '2016/01/31')", Seq(null))
    verifyResult("select _ymdint_between('20160201', '20160131')", Seq(null))
    verifyResult("select _ymdint_between('str1', 'str2')", Seq("null"))
  }

  test("test codegen") {
    val schema = StructType(List(
      StructField("date1", DateType),
      StructField("date2", DateType),
      StructField("date1_string", StringType),
      StructField("date2_string", StringType)
    ))
    val rdd = sc.parallelize(Seq(Row(Date.valueOf("1990-04-30"), Date.valueOf("2003-02-05"), "1990-04-30", "2003-02-05")))

    spark.sqlContext.createDataFrame(rdd, schema).createOrReplaceGlobalTempView("test_ymdint_between")
    verifyResult("select _YMDINT_BETWEEN(date1, date2) from global_temp.test_ymdint_between", Seq("120906"))
    verifyResult("select _ymdint_between(date1, date2_string) from global_temp.test_ymdint_between", Seq("120906"))
    verifyResult("select _ymdint_between(date1_string, date2) from global_temp.test_ymdint_between", Seq("120906"))
    verifyResult("select _ymdint_between(date1_string, date2_string) from global_temp.test_ymdint_between", Seq("120906"))
  }

  def verifyResult(sql: String, expect: Seq[String]): Unit = {
    val actual = spark.sql(sql).collect().map(row => row.toString()).mkString(",")
    assert(actual == "[" + expect.mkString(",") + "]")
  }
}
