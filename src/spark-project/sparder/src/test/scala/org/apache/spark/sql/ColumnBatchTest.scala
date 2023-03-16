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

import java.sql.{Date, Timestamp}

import org.apache.kylin.guava30.shaded.common.collect.Lists
import org.apache.calcite.avatica.util.DateTimeUtils.ymdToUnixDate
import org.apache.spark.sql.catalyst.expressions.KapSubtractMonths
import org.apache.spark.sql.catalyst.util.KapDateTimeUtils
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

// scalastyle:off
class ColumnBatchTest extends SparderBaseFunSuite with SharedSparkSession {
  test("basic") {
    val schema = StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("birth", DateType, nullable = true),
        StructField("time", TimestampType, nullable = true)
      ))

    val data = Seq(
      // only String : Caused by: java.lang.RuntimeException: java.lang.String is not a valid external type for schema of date
      Row(1,
        Date.valueOf("2012-12-12"),
        Timestamp.valueOf("2016-09-30 03:03:00")),
      Row(2,
        Date.valueOf("2016-12-14"),
        Timestamp.valueOf("2016-12-14 03:03:00"))
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    //    df.select(from_utc_timestamp(to_utc_timestamp(lit("2012-12-12 00:00:00.12345"), TimeZone.getTimeZone("UTC").getID), TimeZone.getDefault.getID).cast(LongType)).take(1).foreach(println)
    df.select(org.apache.spark.sql.functions.add_months(lit("2012-01-31"), 1))
      .take(1)
      .foreach(println)
    df.select(
      org.apache.spark.sql.functions.add_months(lit("2012-01-31 10:10:10"),
        1))
      .take(1)
      .foreach(println)
    //    df.select(from_utc_timestamp(to_utc_timestamp(lit("2012-12-12 00:00:00"), TimeZone.getTimeZone("UTC").getID), TimeZone.getDefault.getID).cast(LongType)).take(1).foreach(println)
    //    df.select(from_utc_timestamp(to_utc_timestamp(lit("2012-12-11 16:00:00"), TimeZone.getTimeZone("UTC").getID), TimeZone.getDefault.getID).cast(LongType)).take(1).foreach(println)
    spark.close()
  }

  test("addMonths") {
    println(KapDateTimeUtils.dateAddMonths(ymdToUnixDate(2012, 3, 31), 23))
    println(ymdToUnixDate(2012, 2, 29))
    println(ymdToUnixDate(2014, 2, 28))

  }

  ignore("KapSubtractMonths") {
    val df = mockDFForLit
    val tsc1 = lit("2012-11-12 12:12:12.0").cast(TimestampType)
    val tsc2 = lit("2012-12-12 12:12:12.0").cast(TimestampType)
    val column = Column(KapSubtractMonths(tsc1.expr, tsc2.expr))
    assert(true, df.select(column).head().getInt(0) == -1)
  }

  def mockDFForLit: DataFrame = {
    val row = Row("a")
    val df = spark.createDataFrame(
      Lists.newArrayList(row),
      StructType(Array(StructField("m", StringType, nullable = true))))
    df
  }
}
