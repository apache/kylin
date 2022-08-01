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

import org.apache.kylin.common.util.ByteBufferOutputStream

import java.io.DataOutputStream
import java.nio.ByteBuffer
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, Literal}
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.sql.types.{BinaryType, DateType, LongType}
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.JavaConverters._

class IntersectCountSuite extends SparderBaseFunSuite {
  private val byteBuffer: ByteBuffer = ByteBuffer.allocate(1024)

  private def serialize(bitmap: Roaring64NavigableMap): Array[Byte] = {
    byteBuffer.clear()
    val dos = new DataOutputStream(new ByteBufferOutputStream(byteBuffer))
    bitmap.serialize(dos)
    dos.close()
    byteBuffer.array().slice(0, byteBuffer.position())
  }


  test("return correct result when process row in happy path") {
    val child1 = BoundReference(0, BinaryType, nullable = true)
    val child2 = BoundReference(1, DateType, nullable = true)
    val child3 = Literal(Array(1, 2))
    val count = IntersectCount(child1, child2, child3, Literal(IntersectCount.RAW_STRING), LongType, "\\|")
    val buffer = count.createAggregationBuffer()

    val row = new GenericInternalRow(2)
    row.update(0, null)
    row.update(1, 1)
    count.update(buffer, row)

    val bitmap1 = new Roaring64NavigableMap()
    bitmap1.add(1, 3, 5, 7, 9)
    row.update(0, serialize(bitmap1))
    row.update(1, 1)
    count.update(buffer, row)

    val bitmap2 = new Roaring64NavigableMap()
    bitmap2.add(2, 4, 5, 7, 9)
    row.update(0, serialize(bitmap2))
    row.update(1, 2)
    count.update(buffer, row)

    val bitmap3 = new Roaring64NavigableMap()
    bitmap3.add(1, 3, 6, 8, 10)
    row.update(0, serialize(bitmap3))
    row.update(1, 3)
    count.update(buffer, row)

    val bitmap4 = new Roaring64NavigableMap()
    bitmap4.add(1, 3, 5, 7, 9)
    row.update(0, serialize(bitmap4))
    row.update(1, null)
    count.update(buffer, row)


    val bitmap5 = new Roaring64NavigableMap()
    bitmap5.add(4)
    val otherBuffer = new IntersectBitmapCounter
    otherBuffer.map().put("1", bitmap5)
    count.merge(buffer, otherBuffer)

    assert(count.eval(buffer) == 4L)
  }

  test("return 0 when array size is 0") {
    val child1 = BoundReference(0, BinaryType, nullable = true)
    val child2 = BoundReference(1, DateType, nullable = true)
    val child3 = Literal(Array.empty[Int])
    val count = IntersectCount(child1, child2, child3, Literal(IntersectCount.RAW_STRING), LongType, "\\|")
    val buffer = count.createAggregationBuffer()

    val row = new GenericInternalRow(2)
    val bitmap1 = new Roaring64NavigableMap()
    bitmap1.add(1, 3, 5, 7, 9)
    row.update(0, serialize(bitmap1))
    row.update(1, 1)
    count.update(buffer, row)
    count.update(buffer, row)

    assert(count.eval(buffer) == 0L)
  }

  test("return 0 when map size is not equal to array size") {
    val child1 = BoundReference(0, BinaryType, nullable = true)
    val child2 = BoundReference(1, DateType, nullable = true)
    val child3 = Literal(Array(1, 2))
    val count = IntersectCount(child1, child2, child3, Literal(IntersectCount.RAW_STRING), LongType, "\\|")
    val buffer = count.createAggregationBuffer()

    val row = new GenericInternalRow(2)
    val bitmap1 = new Roaring64NavigableMap()
    bitmap1.add(1, 3, 5, 7, 9)
    row.update(0, serialize(bitmap1))
    row.update(1, 1)
    count.update(buffer, row)
    count.update(buffer, row)

    val bitmap2 = new Roaring64NavigableMap()
    bitmap2.add(2, 4, 6, 8, 10)
    row.update(0, serialize(bitmap2))
    row.update(1, 1)
    count.update(buffer, row)
    count.update(buffer, row)

    assert(count.eval(buffer) == 0L)
  }

  test("serialize and deserialize counter") {
    val child1 = BoundReference(0, BinaryType, nullable = true)
    val child2 = BoundReference(1, DateType, nullable = true)
    val child3 = Literal(Array(1, 2))
    val count = IntersectCount(child1, child2, child3, Literal(IntersectCount.RAW_STRING), LongType, "\\|")
    val counter = new IntersectBitmapCounter()
    val map1 = new Roaring64NavigableMap()
    map1.add(1, 3, 5, 7, 9)
    counter.map().put("1", map1)
    val map2 = new Roaring64NavigableMap()
    map2.add(2, 4, 6, 8, 10)
    counter.map().put("2", map2)
    val bytes = count.serialize(counter)
    val deCounter = count.deserialize(bytes)
    assert(deCounter.map().keySet() == counter.map().keySet())
    assert(deCounter.map().values().asScala sameElements counter.map().values().asScala)
  }
}
