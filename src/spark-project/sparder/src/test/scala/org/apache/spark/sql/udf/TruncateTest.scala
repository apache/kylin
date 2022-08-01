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
import org.apache.spark.sql.catalyst.expressions.Truncate
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{FunctionEntity, Row}
import org.scalatest.BeforeAndAfterAll

class TruncateTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    val function = FunctionEntity(expression[Truncate]("TRUNCATE"))
    spark.sessionState.functionRegistry.registerFunction(function.name, function.info, function.builder)
  }

  test("test truncate long") {
    verifyResult("select truncate(12345L, 10)", Seq("12345"))
  }

  test("test truncate int") {
    verifyResult("select truncate(12345, 10)", Seq("12345"))
  }

  test("test truncate double") {
    verifyResult("select truncate(123.45d, 3)", Seq("123.45"))
    verifyResult("select truncate(123.45d, 2)", Seq("123.45"))
    verifyResult("select truncate(123.45d, 1)", Seq("123.4"))
    verifyResult("select truncate(123.45d, 0)", Seq("123.0"))
  }

  test("test truncate decimal") {
    verifyResult("select truncate(123.45, 3)", Seq("123.45"))
    verifyResult("select truncate(123.45, 2)", Seq("123.45"))
    verifyResult("select truncate(123.45, 1)", Seq("123.40"))
    verifyResult("select truncate(123.45, 0)", Seq("123.00"))
  }

  test("test null") {
    verifyResult("select truncate(null, 3)", Seq("null"))
    verifyResult("select truncate(12345L, null)", Seq("null"))
    verifyResult("select truncate(12345, null)", Seq("null"))
    verifyResult("select truncate(123.45d, null)", Seq("null"))
    verifyResult("select truncate(123.45, null)", Seq("null"))
  }

  test("test codegen") {
    val schema = StructType(List(
      StructField("c_int", IntegerType),
      StructField("c_long", LongType),
      StructField("c_double", DoubleType),
      StructField("c_decimal", DecimalType(5, 2)),
      StructField("c_int2", IntegerType)

    ))
    val rdd = sc.parallelize(Seq(
      Row(12345, 12345L, 123.45d, BigDecimal.apply(123.45), 1)
    ))
    spark.sqlContext.createDataFrame(rdd, schema).createOrReplaceGlobalTempView("test_truncate")
    verifyResult("select truncate(c_int, c_int2) from global_temp.test_truncate", Seq("12345"))
    verifyResult("select truncate(c_long, c_int2) from global_temp.test_truncate", Seq("12345"))
    verifyResult("select truncate(c_double, c_int2) from global_temp.test_truncate", Seq("123.4"))
    verifyResult("select truncate(c_decimal, c_int2) from global_temp.test_truncate", Seq("123.40"))
  }

  def verifyResult(sql: String, expect: Seq[String]): Unit = {
    val actual = spark.sql(sql).collect().map(_.toString()).mkString(",")
    assert(actual == "[" + expect.mkString(",") + "]")
  }
}
