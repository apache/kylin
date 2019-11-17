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

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.{Input, KryoDataInput, KryoDataOutput, Output}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types._
import org.roaringbitmap.longlong.Roaring64NavigableMap

// scalastyle:off
@ExpressionDescription(usage = "PreciseCountDistinct(expr)")
@SerialVersionUID(1)
sealed abstract class BasicPreciseCountDistinct(
  child: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Roaring64NavigableMap] with Serializable with Logging {


  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  override def createAggregationBuffer(): Roaring64NavigableMap = new Roaring64NavigableMap()

  override def merge(buffer: Roaring64NavigableMap, input: Roaring64NavigableMap): Roaring64NavigableMap = {
    buffer.or(input)
    buffer
  }

  var array: Array[Byte] = _
  var output: Output = _

  override def serialize(buffer: Roaring64NavigableMap): Array[Byte] = {
    try {
      if (array == null) {
        array = new Array[Byte](1024 * 1024)
        output = new Output(array)
      }
      buffer.runOptimize()
      output.clear()
      val dos = new KryoDataOutput(output)
      buffer.serialize(dos)
      val i = output.position()
      output.close()
      array.slice(0, i)
    } catch {
      case th: KryoException if th.getMessage.contains("Buffer overflow") =>
        logInfo(s"Resize buffer size to ${array.length * 2}")
        array = new Array[Byte](array.length * 2)
        output.setBuffer(array)
        serialize(buffer)
      case th =>
        throw th
    }
  }

  override def deserialize(bytes: Array[Byte]): Roaring64NavigableMap = {
    val bitMap = new Roaring64NavigableMap()
    if (bytes.nonEmpty) {
      bitMap.deserialize(new KryoDataInput(new Input(bytes)))
    }
    bitMap
  }

  override val prettyName: String = "precise_count_distinct"
}

@SerialVersionUID(1)
case class EncodePreciseCountDistinct(
  child: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
  extends BasicPreciseCountDistinct(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = BinaryType

  override def update(buffer: Roaring64NavigableMap, input: InternalRow): Roaring64NavigableMap = {
    val colValue = child.eval(input)
    if (colValue != null) {
      buffer.add(colValue.asInstanceOf[Long])
    }
    buffer
  }

  override def eval(buffer: Roaring64NavigableMap): Any = {
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

@SerialVersionUID(1)
case class ReusePreciseCountDistinct(
  child: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
  extends BasicPreciseCountDistinct(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = BinaryType

  override def update(buffer: Roaring64NavigableMap, input: InternalRow): Roaring64NavigableMap = {
    val colValue = child.eval(input)
    buffer.or(deserialize(colValue.asInstanceOf[Array[Byte]]))
    buffer
  }

  override def eval(buffer: Roaring64NavigableMap): Any = {
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

@SerialVersionUID(1)
case class PreciseCountDistinct(
  child: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
  extends BasicPreciseCountDistinct(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = LongType

  override def update(buffer: Roaring64NavigableMap, input: InternalRow): Roaring64NavigableMap = {
    val colValue = child.eval(input)
    buffer.or(deserialize(colValue.asInstanceOf[Array[Byte]]))
    buffer
  }

  override def eval(buffer: Roaring64NavigableMap): Any = {
    buffer.getLongCardinality
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

