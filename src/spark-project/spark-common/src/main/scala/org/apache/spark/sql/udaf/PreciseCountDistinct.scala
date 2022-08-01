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

import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, _}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.roaringbitmap.longlong.Roaring64NavigableMap

// scalastyle:off
@ExpressionDescription(usage = "PreciseCountDistinct(expr)")
@SerialVersionUID(1)
sealed abstract class BasicPreciseCountDistinct(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Roaring64NavigableMap] with Serializable with LogEx {


  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  override def createAggregationBuffer(): Roaring64NavigableMap = new Roaring64NavigableMap()

  override def merge(buffer: Roaring64NavigableMap, input: Roaring64NavigableMap): Roaring64NavigableMap = {
    buffer.naivelazyor(input)
    buffer
  }

  override def serialize(buffer: Roaring64NavigableMap): Array[Byte] = {
    if (buffer.isInstanceOf[PlaceHolderBitmap]) {
       Array.empty[Byte]
    } else {
      buffer.repairAfterLazy()
      buffer.runOptimize()
      val size : Int = buffer.serializedSizeInBytes().asInstanceOf[Int]
      BitmapSerAndDeSerObj.serialize(buffer, size)
    }
  }

  override def deserialize(bytes: Array[Byte]): Roaring64NavigableMap = {
    if (bytes != null && bytes.nonEmpty) {
      BitmapSerAndDeSerObj.deserialize(bytes)
    } else {
      new PlaceHolderBitmap
    }
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

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
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
    buffer.naivelazyor(deserialize(colValue.asInstanceOf[Array[Byte]]))
    buffer
  }

  override def eval(buffer: Roaring64NavigableMap): Any = {
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override val prettyName: String = "bitmap_or"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

@SerialVersionUID(1)
case class PreciseCountDistinct(
    child: Expression,
    dataType: DataType,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends BasicPreciseCountDistinct(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression, dataType: DataType) = this(child, dataType, 0, 0)

  override def update(buffer: Roaring64NavigableMap, input: InternalRow): Roaring64NavigableMap = {
    val colValue = child.eval(input)
    buffer.naivelazyor(deserialize(colValue.asInstanceOf[Array[Byte]]))
    buffer
  }

  override def eval(buffer: Roaring64NavigableMap): Any = {
    buffer.repairAfterLazy()
    dataType match {
      case LongType => buffer.getLongCardinality
      case BinaryType => serialize(buffer)
      case _ => throw new UnsupportedOperationException("Unsupported data type in count distinct")
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

@SerialVersionUID(1)
sealed abstract class PreciseCountDistinctAnd(
    child: Expression,
    dataType: DataType,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends BasicPreciseCountDistinct(child, mutableAggBufferOffset, inputAggBufferOffset) {

  var needDeserialize = true

  def this(child: Expression, dataType: DataType) = this(child, dataType, 0, 0)

  override def createAggregationBuffer(): Roaring64NavigableMap = new PlaceHolderBitmap

  override def update(buffer: Roaring64NavigableMap, input: InternalRow): Roaring64NavigableMap = {
    val colValue = child.eval(input)
    if (buffer.isInstanceOf[PlaceHolderBitmap]) {
      deserialize(colValue.asInstanceOf[Array[Byte]])
    } else {
      if (!buffer.isEmpty) {
        buffer.and(deserialize(colValue.asInstanceOf[Array[Byte]]))
      }  else {
        needDeserialize = false
      }
      buffer
    }
  }

  override def deserialize(bytes: Array[Byte]): Roaring64NavigableMap = {
    if(needDeserialize) {
      super.deserialize(bytes)
    } else {
      new Roaring64NavigableMap
    }
  }

  override def merge(buffer: Roaring64NavigableMap, input: Roaring64NavigableMap): Roaring64NavigableMap = {
    if (buffer.isInstanceOf[PlaceHolderBitmap]) {
      input
    } else {
      if (input.isInstanceOf[PlaceHolderBitmap]) {
        buffer
      } else {
        if (!buffer.isEmpty) {
          buffer.and(input)
        } else {
          needDeserialize = false
        }
        buffer
      }
    }
  }

  override def eval(buffer: Roaring64NavigableMap): Any = {
    dataType match {
      case LongType => buffer.getLongCardinality
      case BinaryType => serialize(buffer)
      case ArrayType(LongType, false) =>

        val cardinality = buffer.getIntCardinality
        if (cardinality > 10000000) {
          throw new UnsupportedOperationException("Unsupported data type in count distinct")
        }
        val longs = new Array[Long](cardinality)
        var id = 0
        val iterator = buffer.iterator()
        while(iterator.hasNext) {
          longs(id) = iterator.next()
          id += 1
        }
        new GenericArrayData(longs)
      case _ => throw new UnsupportedOperationException("Unsupported data type in count distinct")
    }
  }
}

class PlaceHolderBitmap extends Roaring64NavigableMap

@SerialVersionUID(1)
case class PreciseCountDistinctAndValue(
    child: Expression,
    dataType: DataType = LongType,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends PreciseCountDistinctAnd(child, dataType, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, LongType, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = "bitmap_and_value"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

@SerialVersionUID(1)
case class PreciseCountDistinctAndArray(
    child: Expression,
    dataType: DataType = ArrayType(LongType, containsNull = false),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends PreciseCountDistinctAnd(child, dataType, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, ArrayType(LongType, containsNull = false), 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = "bitmap_and_ids"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

case class PreciseCardinality(override val child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def dataType: DataType = LongType
  override def prettyName: String = "bitmap_cardinality"

  override def nullSafeEval(input: Any): Long = {
    val data = input.asInstanceOf[Array[Byte]]
    BitmapSerAndDeSerObj.deserialize(data).getLongCardinality
  }

  override protected def withNewChildInternal(newChild: Expression): PreciseCardinality =
    copy(child = newChild)
}

@SerialVersionUID(1)
case class PreciseBitmapBuildBase64WithIndex(
                                 child: Expression,
                                 dataType: DataType,
                                 mutableAggBufferOffset: Int = 0,
                                 inputAggBufferOffset: Int = 0)
  extends BasicPreciseCountDistinct(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression, dataType: DataType) = this(child, dataType, 0, 0)

  override def update(buffer: Roaring64NavigableMap, input: InternalRow): Roaring64NavigableMap = {
    val colValue = child.eval(input)
    buffer.naivelazyor(deserialize(colValue.asInstanceOf[Array[Byte]]))
    buffer
  }

  override def eval(buffer: Roaring64NavigableMap): Any = {
    buffer.repairAfterLazy()
    val encodeValue = org.apache.commons.codec.binary.Base64.encodeBase64String(serialize(buffer))
    UTF8String.fromString(encodeValue)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = "bitmap_build_with_index"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

@SerialVersionUID(1)
case class PreciseBitmapBuildBase64Decode(override val child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def dataType: DataType = StringType
  override def prettyName: String = "bitmap_build_decode"

  override def nullSafeEval(input: Any): UTF8String = {
    val data = input.asInstanceOf[Array[Byte]]
    val encodeValue = org.apache.commons.codec.binary.Base64.encodeBase64String(data)
    UTF8String.fromString(encodeValue)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

@SerialVersionUID(1)
case class PreciseBitmapBuildPushDown(child: Expression,
                                       mutableAggBufferOffset: Int = 0,
                                       inputAggBufferOffset: Int = 0)
  extends BasicPreciseCountDistinct(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override val prettyName: String = "bitmap_build"

  override def dataType: DataType = StringType

  override def update(buffer: Roaring64NavigableMap, input: InternalRow): Roaring64NavigableMap = {
    val inputValue = child.eval(input)
    if (inputValue != null) {
      var colValue = 0L
      inputValue match {
        case value: Integer =>
          colValue = value.longValue()
        case value: Long =>
          colValue = value.longValue()
        case _ => throw new UnsupportedOperationException("Unsupported data type in bitmap_build")
      }
      buffer.add(colValue)
    }
    buffer
  }

  override def eval(buffer: Roaring64NavigableMap): Any = {
    val bitmapBytes = serialize(buffer)
    val encodeValue = org.apache.commons.codec.binary.Base64.encodeBase64String(bitmapBytes)
    UTF8String.fromString(encodeValue)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}
