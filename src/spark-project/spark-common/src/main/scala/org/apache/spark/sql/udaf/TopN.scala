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

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.{Input, KryoDataInput, KryoDataOutput, Output}
import org.apache.kylin.measure.topn.TopNCounter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

@SerialVersionUID(1)
sealed abstract class BaseTopN(precision: Int,
                               internalSchema: StructType,
                               mutableAggBufferOffset: Int = 0,
                               inputAggBufferOffset: Int = 0
                              ) extends TypedImperativeAggregate[TopNCounter[Seq[Any]]] with Serializable with Logging {
  lazy val serializers: Seq[NullSafeValueSerializer] =
    Seq(new DoubleSerializer) ++ internalSchema.drop(1).map(_.dataType).map {
      case BooleanType => new BooleanSerializer
      case ByteType => new ByteSerializer
      case ShortType => new ShortSerializer
      case IntegerType => new IntegerSerializer
      case FloatType => new FloatSerializer
      case LongType => new LongSerializer
      case DoubleType => new DoubleSerializer
      case TimestampType => new LongSerializer
      case DateType => new IntegerSerializer
      case StringType => new StringSerializer
      case dt => throw new UnsupportedOperationException("Unsupported TopN dimension type: " + dt)
    }

  override def createAggregationBuffer(): TopNCounter[Seq[Any]] = new TopNCounter[Seq[Any]](precision * TopNCounter.EXTRA_SPACE_RATE)

  override def merge(buffer: TopNCounter[Seq[Any]], input: TopNCounter[Seq[Any]]): TopNCounter[Seq[Any]] = {
    input.getCounterList.asScala.foreach { c =>
      buffer.offer(c.getItem, c.getCount)
    }
    buffer
  }

  override def eval(buffer: TopNCounter[Seq[Any]]): Any = {
    buffer.sortAndRetain()
    val seq = buffer.getCounterList.asScala.map(
      entry => InternalRow(entry.getCount, InternalRow(entry.getItem: _*))
    )
    ArrayData.toArrayData(seq)
  }

  var array: Array[Byte] = _
  var output: Output = _

  override def serialize(topNCounter: TopNCounter[Seq[Any]]): Array[Byte] = {
    try {
      if (topNCounter != null) {
        if (array == null) {
          array = new Array[Byte](1024 * 1024)
          output = new Output(array)
        }
        output.clear()
        val out = new KryoDataOutput(output)
        topNCounter.sortAndRetain()
        val counters = topNCounter.getCounterList
        out.writeInt(counters.size())
        counters.asScala.foreach { counter =>
          val values = Seq(counter.getCount) ++ counter.getItem
          values.zip(serializers).foreach { case (value, ser) =>
            ser.serialize(out, value)
          }
        }
        val i = output.position()
        output.close()
        array.slice(0, i)
      } else {
        Array.empty[Byte]
      }
    } catch {
      case th: KryoException if th.getMessage.contains("Buffer overflow") =>
        logWarning(s"Resize buffer size to ${array.length * 2}")
        array = new Array[Byte](array.length * 2)
        output.setBuffer(array)
        serialize(topNCounter)
      case th =>
        throw th
    }
  }

  override def deserialize(bytes: Array[Byte]): TopNCounter[Seq[Any]] = {
    val topNCounter = new TopNCounter[Seq[Any]](precision * TopNCounter.EXTRA_SPACE_RATE)
    if (bytes.nonEmpty) {
      val in = new KryoDataInput(new Input(bytes))
      val size = in.readInt()
      for (_ <- 0 until size) {
        val values = serializers.map(_.deserialize(in))
        val item = values.drop(1)
        if (values.head == null) {
          topNCounter.offer(item, null)
        } else {
          topNCounter.offer(item, values.head.asInstanceOf[Double])
        }
      }
    }
    topNCounter
  }

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(
    StructType(Seq(
      StructField("measure", DoubleType),
      StructField("dim", StructType(internalSchema.fields.drop(1)))
    )))
}

case class ReuseTopN(
                      precision: Int,
                      internalSchema: StructType,
                      child: Expression,
                      mutableAggBufferOffset: Int = 0,
                      inputAggBufferOffset: Int = 0)
  extends BaseTopN(precision, internalSchema, mutableAggBufferOffset, inputAggBufferOffset) {
  val dimType: StructType = StructType(internalSchema.fields.drop(1))
  val innerType: StructType = StructType(Seq(
    StructField("measure", DoubleType),
    StructField("dim", dimType)
  ))

  override def update(buffer: TopNCounter[Seq[Any]], input: InternalRow): TopNCounter[Seq[Any]] = {
    val datum = child.eval(input).asInstanceOf[ArrayData].toArray[UnsafeRow](innerType)
    datum.foreach { data =>
      val value = data.getDouble(0)
      val dims = data.get(1, dimType).asInstanceOf[InternalRow]
      val item = dimType.fields.map(_.dataType).zipWithIndex.map {
        case (StringType, index) =>
          val result = dims.get(index, StringType)
          if (null != result) {
            dims.getString(index)
          } else {
            result
          }
        case (dataType, index) =>
          dims.get(index, dataType)
      }.toSeq
      buffer.offer(item, value)
    }
    buffer
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = child :: Nil

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

case class EncodeTopN(
                       precision: Int,
                       internalSchema: StructType,
                       measure: Expression,
                       dimensions: Seq[Expression],
                       mutableAggBufferOffset: Int = 0,
                       inputAggBufferOffset: Int = 0)
  extends BaseTopN(precision, internalSchema, mutableAggBufferOffset, inputAggBufferOffset) {
  override def update(counter: TopNCounter[Seq[Any]], input: InternalRow): TopNCounter[Seq[Any]] = {
    val m = measure.eval(input)
    val dims = dimensions.map {
      case str: Expression if str.dataType.isInstanceOf[StringType] =>
        val value = str.eval(input)
        if (value != null) {
          value.toString
        } else {
          null
        }
      case str =>
        str.eval(input)
    }
    if (m == null) {
      counter.offer(dims, null)
    } else {
      counter.offer(dims, m.toString.toDouble)
    }
    counter
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(measure) ++ dimensions

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}
