/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.spark.sql.udaf

import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow}
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.sql.types.LongType

class UDAFFunSuite extends SparderBaseFunSuite{

  test("dser and ser hllc") {
    val value = BoundReference(0, LongType, nullable = true)
    Seq(8, 10, 16).map { precision =>
      val distinct = EncodeApproxCountDistinct(value, precision)
      val state = distinct.createAggregationBuffer()
      Range(0, 100).foreach{
        num =>
          val row = new GenericInternalRow(1)
          row.update(0, num.toLong)
          distinct.update(state, row)
      }
      var bytes = distinct.serialize(state)
      assert(distinct.deserialize(bytes).hllc.getState == state.hllc.getState)
      val state2 = distinct.createAggregationBuffer()
      Range(100, 200).foreach{
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
      assert(distinct.merge(afterMerge, state2).hllc.getState ==  state.hllc.getState)
    }
  }
  
  test("dser and ser bitmap") {
    val value = BoundReference(0, LongType, nullable = true)
      val distinct = EncodePreciseCountDistinct(value)
      val state = distinct.createAggregationBuffer()
      Range(0, 100).foreach{
        num =>
          val row = new GenericInternalRow(1)
          row.update(0, num.toLong)
          distinct.update(state, row)
      }
      var bytes = distinct.serialize(state)
      assert(distinct.deserialize(bytes) == state)

    val state2 = distinct.createAggregationBuffer()
      Range(100, 200).foreach{
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
    assert(distinct.merge(afterMerge, state2).getLongCardinality == 200L)
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
