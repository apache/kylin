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

import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.udaf.{BitmapSerAndDeSer, BitmapSerAndDeSerObj}
import org.apache.spark.sql.udf.UdfManager
import org.roaringbitmap.longlong.Roaring64NavigableMap
import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.{Millis, Span}

// scalastyle:off
class CountDistinctTest
  extends SparderBaseFunSuite
    with SharedSparkSession
    with KapQueryUtils
    with TimeLimits {

  import testImplicits._

  override def beforeAll() {
    super.beforeAll()

    UdfManager.create(spark)
  }

  test("test precise_count_distinct without grouping keys") {
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)

    val df = Seq(("a": String, array1),
                 (null, array1),
                 ("b": String, array2),
                 ("a": String, array2)).toDF("col1", "col2")

    checkAnswer(df.coalesce(1).select(precise_count_distinct($"col2")),
                Seq(Row(4)))
    checkBitmapAnswer(df.coalesce(1).select(precise_bitmap_uuid($"col2")), Array(4L))
  }

  test("test precise_count_distinct with grouping keys") {
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)

    val df = Seq(("a": String, array1),
                 ("b": String, array2),
                 ("a": String, array2)).toDF("col1", "col2")
    checkAnswer(
      df.coalesce(1).groupBy($"col1").agg(precise_count_distinct($"col2")),
      Seq(Row("a", 4), Row("b", 3))
    )
    checkBitmapAnswer(
      df.coalesce(1).groupBy($"col1").agg(precise_bitmap_uuid($"col2").as("col")).select("col"),
      Array(4L, 3L)
    )
  }

  test("test precise_count_distinct fallback to sort-based aggregation") {
    spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, 2)
    SparkSession.setActiveSession(spark)
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)
    val array3: Array[Byte] = getBitmapArray(1, 3, 5, 6, 7)
    val df = Seq(("a": String, array1),
                 ("b": String, array2),
                 ("c": String, array3),
                 ("a": String, array2),
                 ("a": String, array3))
      .toDF("col1", "col2")

    checkAnswer(
      df.coalesce(1).groupBy($"col1").agg(precise_count_distinct($"col2")),
      Seq(Row("a", 6), Row("b", 3), Row("c", 5))
    )
    checkBitmapAnswer(
      df.coalesce(1).groupBy($"col1").agg(precise_bitmap_uuid($"col2").as("col")).select("col"),
      Array(6L, 3L, 5L)
    )
  }
  private def checkBitmapAnswer(df: => DataFrame,
                                expectedAnswer: Array[Long]): Unit = {
    val result = df.collect().map(row => BitmapSerAndDeSer.get().deserialize(row.getAs[Array[Byte]](0)).getLongCardinality)
    assert(result sameElements expectedAnswer)
  }

  test("test approx_count_distinct without grouping keys") {
    val array1: Array[Byte] = getHllcArray(1, 2)
    val array2: Array[Byte] = getHllcArray(1, 3, 5)

    val df = Seq(("a": String, array1),
                 (null, array1),
                 ("b": String, array2),
                 ("a": String, array2)).toDF("col1", "col2")

    checkAnswer(df.coalesce(1).select(approx_count_distinct($"col2", 14)),
                Seq(Row(4)))
  }

  test("test approx_count_distinct with grouping keys") {
    val array1: Array[Byte] = getHllcArray(1, 2)
    val array2: Array[Byte] = getHllcArray(1, 3, 5)

    val df = Seq(("a": String, array1),
                 ("b": String, array2),
                 ("a": String, array2)).toDF("col1", "col2")
    checkAnswer(
      df.coalesce(1).groupBy($"col1").agg(approx_count_distinct($"col2", 14)),
      Seq(Row("a", 4), Row("b", 3))
    )
  }

  test("using sql without exception") {
    val array1: Array[Byte] = getBitmapArray(1L, 2L, 234556L, 234556234556L)

    val df = Seq(array1).toDF("col1").coalesce(1)
    df.createOrReplaceTempView("t")

    spark.sql("select bitmap_and_value(col1) from t")
    spark.sql("select bitmap_and_ids(col1) as ids from t")
    spark.sql("select explode(ids) from (select bitmap_and_ids(col1) as ids from t)")
  }

  test("test bitmap_and_ids") {
    val expected = Seq(Row(1L), Row(2L), Row(234556L), Row(234556234556L))
    val array1: Array[Byte] = getBitmapArray(1L, 2L, 234556L, 234556234556L)
    val df0 = Seq(array1).toDF("col1").coalesce(1)
    df0.createOrReplaceTempView("t")
    checkAnswer(
      spark.sql("select explode(ids) from (select bitmap_and_ids(col1) as ids from t)"),
      expected)
  }

  test("why we need PlaceHolderBitmap") {
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)
    val array3: Array[Byte] = getBitmapArray(1, 4)
    val array4: Array[Byte] = getBitmapArray(1, 6, 5)

    val schema = StructType(Seq(
      StructField("col1", StringType),
      StructField("col2", BinaryType)))

    val data = Seq(
      Row("a": String, array1),
      Row("b": String, array2),
      Row("c": String, array3),
      Row("d": String, array4))

    val df1 =  spark.createDataFrame(spark.sparkContext.parallelize(data,4), schema)

    df1.createOrReplaceTempView("t")
    val sql2 = "select bitmap_and_value(col2) as ids from t where col1 = 'a'"

    val expected = Seq(Row(2L))
    checkAnswer(spark.sql(sql2), expected)
  }

  test("test bitmap_or") {
    val expected = Seq(Row("a", 4), Row("b", 3))
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)

    val df0 = Seq(("a": String, array1),
      ("b": String, array2),
      ("a": String, array2)).toDF("col1", "col2").coalesce(1)


    df0.createOrReplaceTempView("t")
    val SQL = "select col1, bitmap_cardinality(bitmap_or(col2)) as col2 from t group by col1"
    checkAnswer(spark.sql(SQL), expected)
  }

  test("test bitmap_and_value") {
    val expected = Seq(Row("a", 1), Row("b", 3))
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)

    val df0 = Seq(("a": String, array1),
      ("b": String, array2),
      ("a": String, array2)).toDF("col1", "col2").coalesce(1)


    df0.createOrReplaceTempView("t")
    val SQL = "select col1, bitmap_and_value(col2) as col2 from t group by col1"
    checkAnswer(spark.sql(SQL), expected)
  }

  test("test BitmapSerAndDeSerObj serialize should not overflow") {
    // BitmapSerAndDeSerObj.serialize may loop infinitely
    failAfter(Span(10000, Millis)) {
      val bitmap = new Roaring64NavigableMap
      bitmap.add(1L, 2L, 234556L, 234556234556L)
      val array = BitmapSerAndDeSerObj.serialize(bitmap, 1)
      assert(array.length > 1)
      val de_bitmap = BitmapSerAndDeSerObj.deserialize(array)
      assertResult(4L)(de_bitmap.getLongCardinality)
      assert(de_bitmap.contains(234556234556L))
    }
  }

  test("test precise_count_distinct_decode") {
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)

    val df = Seq(("a": String, array1),
      (null, array1),
      ("b": String, array2),
      ("a": String, array2)).toDF("col1", "col2")
    checkAnswer(df.coalesce(1).select(precise_count_distinct_decode($"col2")),
      Seq(Row(2), Row(2), Row(3), Row(3)))
  }

  test("test approx_count_distinct_decode") {
    val array1: Array[Byte] = getHllcArray(1, 2)
    val array2: Array[Byte] = getHllcArray(1, 3, 5)

    val df = Seq(("a": String, array1),
      (null, array1),
      ("b": String, array2),
      ("a": String, array2)).toDF("col1", "col2")

    checkAnswer(df.coalesce(1).select(approx_count_distinct_decode($"col2", 14)),
      Seq(Row(2), Row(2), Row(3), Row(3)))
  }
}
