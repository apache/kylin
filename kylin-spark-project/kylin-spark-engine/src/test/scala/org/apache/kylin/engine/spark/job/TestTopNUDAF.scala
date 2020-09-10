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

package org.apache.kylin.engine.spark.job

import java.sql.{Date, Timestamp}

import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TestTopNUDAF extends SparderBaseFunSuite with SharedSparkSession {
  test("basic") {

    val schema = StructType(Array(
        StructField("rowKey", IntegerType, nullable = true),
        StructField("priceDecimal", DecimalType(18, 3), nullable = true),
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("flag", BooleanType, nullable = true),
        StructField("birth", DateType, nullable = true),
        StructField("time", TimestampType, nullable = true)
      ))

    val data = Seq(
      Row(1, BigDecimal(10.2), 1, "n1", true, Date.valueOf("2012-12-12"), Timestamp.valueOf("2016-09-30 03:03:00")),
      Row(1, BigDecimal(20.2), 1, "n1", true, Date.valueOf("2012-12-12"), Timestamp.valueOf("2016-09-30 03:03:00")),
      Row(2, BigDecimal(30.2), 1, "n1", true, Date.valueOf("2012-12-12"), Timestamp.valueOf("2016-09-30 03:03:00")),
      Row(2, BigDecimal(2000), 2, "n2", true, Date.valueOf("2016-12-14"), Timestamp.valueOf("2016-12-14 03:03:00")),
      Row(3, BigDecimal(3000), 2, "n2", true, Date.valueOf("2016-12-14"), Timestamp.valueOf("2016-12-14 03:03:00")),
      Row(3, BigDecimal(1000), null, null, null, null, null),
      Row(4, null, 3, "n2", true, Date.valueOf("2016-12-14"), Timestamp.valueOf("2016-12-14 03:03:00")),
      Row(4, null, null, null, null, null, null)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    spark.udf.register("topn", new TopNUDAF(DataType.getType("topn(10)"), StructType(schema.drop(1)), true))
    spark.udf.register("topn_afterAgg", new TopNUDAF(DataType.getType("topn(10)"), StructType(schema.drop(1)), false))
    val cols = schema.fields.drop(1).map(f => col(f.name))
    val agg1 = callUDF("topn", cols: _*)
    val firstAggreaged = df.groupBy(col("rowKey")).agg(agg1).sort(col("rowKey"))
    val r1 = firstAggreaged.collect()
    // scalastyle:off
    assert(r1.apply(0).toString() == "[1,WrappedArray([30.4,[1,n1,true,2012-12-12,2016-09-30 03:03:00.0]])]")
    assert(r1.apply(1).toString() == "[2,WrappedArray([2000.0,[2,n2,true,2016-12-14,2016-12-14 03:03:00.0]], [30.2,[1,n1,true,2012-12-12,2016-09-30 03:03:00.0]])]")
    assert(r1.apply(2).toString() == "[3,WrappedArray([3000.0,[2,n2,true,2016-12-14,2016-12-14 03:03:00.0]], [1000.0,[null,null,null,null,null]])]")
    assert(
      r1.apply(3).toString() == "[4,WrappedArray([null,[3,n2,true,2016-12-14,2016-12-14 03:03:00.0]], [null,[null,null,null,null,null]])]"
    )
    // scalastyle:on

    val agg2 = callUDF("topn_afterAgg", col("topn(priceDecimal, id, name, flag, birth, time)"))
    val r2 = firstAggreaged.agg(agg2).collect()

    assert(
      r2.apply(0).toString() ==
        "[WrappedArray(" +
          "[5000.0,[2,n2,true,2016-12-14,2016-12-14 03:03:00.0]], " +
          "[1000.0,[null,null,null,null,null]], " +
          "[60.599999999999994,[1,n1,true,2012-12-12,2016-09-30 03:03:00.0]], " +
          "[null,[3,n2,true,2016-12-14,2016-12-14 03:03:00.0]])]"
    )
  }
}
