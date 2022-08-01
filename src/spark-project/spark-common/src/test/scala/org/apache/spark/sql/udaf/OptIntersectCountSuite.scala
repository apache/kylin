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

package org.apache.spark.sql.udaf

import com.esotericsoftware.kryo.io.{Input, KryoDataInput}
import org.apache.spark.sql.Column
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions.{col, split, _}
import org.roaringbitmap.longlong.Roaring64NavigableMap

class OptIntersectCountSuite extends SparderBaseFunSuite with SharedSparkSession {

  import testImplicits._

  test("happy path") {
    val singersDF = Seq(
      (19, 1L, "rich|tall"),
      (19, 2L, "handsome|rich"),
      (21, 2L, "rich|tall|handsome")
    ).toDF("age", "id", "tag")

    val df = singersDF.withColumn(
      "tag",
      split(col("tag"), "\\|")
    ).repartition(1)

    val data = df.groupBy(col("age")).agg(new Column(OptIntersectCount(col("id").expr, col("tag").expr)
      .toAggregateExpression())
      .alias("intersect_count"))
    val ret = data.select(col("age"), explode('intersect_count)).collect.map(rows => {
      s"${rows.get(0).toString}, ${rows.get(1).toString}, ${dser(rows.get(2).asInstanceOf[Array[Byte]])}"
    }).mkString("|")
    assert("19, tall, {1}|19, rich, {1,2}|19, handsome, {2}|21, tall, {2}|21, rich, {2}|21, handsome, {2}".equals(ret))
  }

  def dser(bytes: Array[Byte]): Roaring64NavigableMap = {
    val bitmap = new Roaring64NavigableMap
    bitmap.deserialize(new KryoDataInput(new Input(bytes)))
    bitmap
  }
}
