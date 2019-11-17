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

import java.nio.ByteBuffer

import org.apache.kylin.measure.hllc.{HLLCAggregator, HLLCounter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
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
      if (storageFormat.nonEmpty) {
        val buffer = ByteBuffer.wrap(storageFormat)
        counter.readRegisters(buffer)
        aggregator.aggregate(counter)
      }
      HLLCCounter.State(aggregator)
    } catch {
      case throwable: Throwable =>
        throwable.printStackTrace()
        println("==============")
        storageFormat.foreach(print)
        println("==============")
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
    buffer.hllc.aggregate(deserialize(colValue.asInstanceOf[Array[Byte]]).hllc.getState)
    buffer
  }

  override def eval(buffer: HLLCCounter.State): Any = {
    buffer.hllc.getState.getCountEstimate
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}