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

import java.nio.ByteBuffer

import org.apache.kylin.measure.hllc.{HLLCAggregator, HLLCounter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._

// scalastyle:off
@ExpressionDescription(usage = "ApproxCountDistinct(expr)")
@SerialVersionUID(1)
abstract sealed class BasicApproxCountDistinct(
  child: Expression,
  precision: Int,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HLLCCounter.State] with Serializable with Logging {

  var serialVersionUID = 1

  def this(child: Expression) = this(child, 0, 0)

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  lazy val buf: ByteBuffer = ByteBuffer.allocate(1024 * 1024)

  override def createAggregationBuffer(): HLLCCounter.State = {
    val aggregator = new HLLCAggregator(precision)
    aggregator.aggregate(new HLLCounter(precision))
    HLLCCounter.State(aggregator)
  }


  override def merge(buffer: HLLCCounter.State, input: HLLCCounter.State): HLLCCounter.State = {
    buffer.hllc.aggregate(input.hllc.getState)
    buffer
  }


  override def serialize(buffer: HLLCCounter.State): Array[Byte] = {
    try {
      buf.clear()
      buffer.hllc.getState.writeRegisters(buf)
      val i = buf.position()
      val bytes = buf.array().slice(0, i)
      bytes
    } catch {
      case throwable: Throwable =>
        throwable.printStackTrace()
        throw throwable
      case other =>
        throw other
    }
  }

  override def deserialize(storageFormat: Array[Byte]): HLLCCounter.State = {
    try {
      val aggregator = new HLLCAggregator(precision)
      val counter = new HLLCounter(precision)
      if (storageFormat != null && storageFormat.nonEmpty) {
        val buffer = ByteBuffer.wrap(storageFormat)
        counter.readRegisters(buffer)
        aggregator.aggregate(counter)
      }
      HLLCCounter.State(aggregator)
    } catch {
      case throwable: Throwable =>
        throwable.printStackTrace()
        if (storageFormat != null) {
          println("==============")
          storageFormat.foreach(print)
          println("==============")
        }
        throw throwable
      case other =>
        throw other
    }
  }

  override val prettyName: String = this.getClass.getName
}

object HLLCCounter {

  case class State(var hllc: HLLCAggregator)

}

@SerialVersionUID(1)
case class EncodeApproxCountDistinct(
  child: Expression,
  precision: Int,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
  extends BasicApproxCountDistinct(child, precision, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = BinaryType

  override def update(buffer: HLLCCounter.State, input: InternalRow): HLLCCounter.State = {
    val colValue = child.eval(input)
    if (colValue != null) {
      buffer.hllc.add(colValue.toString)
    }
    buffer
  }

  override def eval(buffer: HLLCCounter.State): Any = {
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

@SerialVersionUID(1)
case class ReuseApproxCountDistinct(
  child: Expression,
  precision: Int,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
  extends BasicApproxCountDistinct(child, precision, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = BinaryType

  override def update(buffer: HLLCCounter.State, input: InternalRow): HLLCCounter.State = {
    val colValue = child.eval(input)
    buffer.hllc.aggregate(deserialize(colValue.asInstanceOf[Array[Byte]]).hllc.getState)
    buffer
  }

  override def eval(buffer: HLLCCounter.State): Any = {
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

@SerialVersionUID(1)
case class ApproxCountDistinct(
  child: Expression,
  precision: Int,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
  extends BasicApproxCountDistinct(child, precision, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = LongType

  override def update(buffer: HLLCCounter.State, input: InternalRow): HLLCCounter.State = {
    val colValue = child.eval(input)
    val state = deserialize(colValue.asInstanceOf[Array[Byte]]).hllc.getState
    if (state != null) {
      buffer.hllc.aggregate(state);
    }
    buffer
  }

  override def eval(buffer: HLLCCounter.State): Any = {
    buffer.hllc.getState.getCountEstimate
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}