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

import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow}
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.sql.types.LongType

class UDAFFunSuite extends SparderBaseFunSuite {

  test("dser and ser hllc") {
    val value = BoundReference(0, LongType, nullable = true)
    Seq(8, 10, 16).map { precision =>
      val distinct = EncodeApproxCountDistinct(value, precision)
      val state = distinct.createAggregationBuffer()
      Range(0, 100).foreach {
        num =>
          val row = new GenericInternalRow(1)
          row.update(0, num.toLong)
          distinct.update(state, row)
      }
      var bytes = distinct.serialize(state)
      assert(distinct.deserialize(bytes).hllc.getState == state.hllc.getState)
      val state2 = distinct.createAggregationBuffer()
      Range(100, 200).foreach {
        num =>
          val row = new GenericInternalRow(1)
          row.update(0, num.toLong)
          distinct.update(state2, row)
      }
      bytes = distinct.serialize(state2)
      assert(distinct.deserialize(bytes).hllc.getState == state2.hllc.getState)

      val buffer = distinct.createAggregationBuffer()

      val afterMerge = distinct.merge(buffer, state)
      assert(afterMerge.hllc.getState == state.hllc.getState)
      state.hllc.aggregate(state2.hllc.getState)
      assert(distinct.merge(afterMerge, state2).hllc.getState == state.hllc.getState)
    }
  }

  test("dser and ser bitmap") {
    val value = BoundReference(0, LongType, nullable = true)
    val distinct = EncodePreciseCountDistinct(value)
    val state = distinct.createAggregationBuffer()
    Range(0, 100).foreach {
      num =>
        val row = new GenericInternalRow(1)
        row.update(0, num.toLong)
        distinct.update(state, row)
    }
    var bytes = distinct.serialize(state)
    assert(distinct.deserialize(bytes) == state)

    val state2 = distinct.createAggregationBuffer()
    Range(100, 200).foreach {
      num =>
        val row = new GenericInternalRow(1)
        row.update(0, num.toLong)
        distinct.update(state2, row)
    }
    bytes = distinct.serialize(state2)
    assert(distinct.deserialize(bytes) == state2)

    val buffer = distinct.createAggregationBuffer()

    val afterMerge = distinct.merge(buffer, state)
    assert(afterMerge.getLongCardinality == 100L)
    val merge = distinct.merge(afterMerge, state2)
    merge.repairAfterLazy()
    assert(merge.getLongCardinality == 200L)
  }

  test("hllc null value") {
    val value = BoundReference(0, LongType, nullable = true)
    val distinct = EncodeApproxCountDistinct(value, 8)
    val state = distinct.createAggregationBuffer()
    val row = new GenericInternalRow(1)
    row.update(0, null)
    distinct.update(state, row)
    assert(state.hllc.getState == distinct.createAggregationBuffer().hllc.getState)
  }

  test("bitmap null value") {
    val value = BoundReference(0, LongType, nullable = true)
    val distinct = EncodePreciseCountDistinct(value)
    val state = distinct.createAggregationBuffer()
    val row = new GenericInternalRow(1)
    row.update(0, null)
    distinct.update(state, row)
    assert(state == distinct.createAggregationBuffer())
  }
}
