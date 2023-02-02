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
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.measure.sumlc.SumLCCounter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types._

/**
 * Build sum_lc measure, has two implements，
 * for non-reuse sum_lc, two input, one for value column, another for date column
 * for reuse sum_lc，construct measure from parent layout
 *
 * @param wrapDataType           return serialize data type
 * @param outputDataType         return calculate data type, see Percentile outputType
 * @param mutableAggBufferOffset default value
 * @param inputAggBufferOffset   default value
 */
sealed abstract class BaseSumLC(wrapDataType: DataType,
                                outputDataType: DataType = BinaryType,
                                mutableAggBufferOffset: Int = 0,
                                inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[SumLCCounter] with Serializable with Logging {
  lazy val serializer: NullSafeValueSerializer = SumLCUtil.getNumericNullSafeSerializerByDataType(wrapDataType)

  override def prettyName: String = "sum_lc"

  override def eval(buffer: SumLCCounter): Any = {
    outputDataType match {
      case BinaryType =>
        serialize(buffer)
      case DecimalType() =>
        if (buffer.getSumLC != null) {
          Decimal.fromDecimal(buffer.getSumLC.asInstanceOf[java.math.BigDecimal])
        } else {
          Decimal.ZERO
        }
      case _ =>
        buffer.getSumLC
    }
  }

  override def createAggregationBuffer(): SumLCCounter = new SumLCCounter()

  override def merge(buffer: SumLCCounter, input: SumLCCounter): SumLCCounter = {
    SumLCCounter.merge(buffer, input)
  }

  override def serialize(buffer: SumLCCounter): Array[Byte] = {
    val array: Array[Byte] = new Array[Byte](1024 * 1024)
    val output: Output = new Output(array)
    serialize(buffer, array, output)
  }

  private def serialize(buffer: SumLCCounter, array: Array[Byte], output: Output): Array[Byte] = {
    try {
      if (buffer == null) {
        Array.empty[Byte]
      } else {
        output.clear()
        val out = new KryoDataOutput(output)
        serializer.serialize(out, buffer.getSumLC)
        out.writeLong(buffer.getTimestamp)
        val mark = output.position()
        output.close()
        array.slice(0, mark)
      }
    } catch {
      case th: KryoException if th.getMessage.contains("Buffer overflow") =>
        logWarning(s"Resize buffer size to ${array.length * 2}")
        val updateArray = new Array[Byte](array.length * 2)
        output.setBuffer(updateArray)
        serialize(buffer, updateArray, output)
      case th =>
        throw th
    }
  }

  override def deserialize(bytes: Array[Byte]): SumLCCounter = {
    SumLCUtil.decodeToSumLCCounter(bytes, serializer)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = outputDataType
}

case class EncodeSumLC(
                        evalCol: Expression,
                        dateCol: Expression,
                        wrapDataType: DataType,
                        outputDataType: DataType = BinaryType,
                        mutableAggBufferOffset: Int = 0,
                        inputAggBufferOffset: Int = 0)
  extends BaseSumLC(wrapDataType, outputDataType, mutableAggBufferOffset, inputAggBufferOffset) {

  override def update(buffer: SumLCCounter, input: InternalRow): SumLCCounter = {
    val columnEvalVal = evalCol.eval(input)
    val columnVal = columnEvalVal match {
      case decimal: Decimal =>
        decimal.toJavaBigDecimal
      case _ =>
        columnEvalVal.asInstanceOf[Number]
    }
    val dateValStr = String.valueOf(dateCol.eval(input)).trim
    val timestampVal = DateFormat.stringToMillis(dateValStr)
    SumLCCounter.merge(buffer, columnVal, timestampVal)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(evalCol, dateCol)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

case class ReuseSumLC(measure: Expression,
                      wrapDataType: DataType,
                      outputDataType: DataType = BinaryType,
                      mutableAggBufferOffset: Int = 0,
                      inputAggBufferOffset: Int = 0)
  extends BaseSumLC(wrapDataType, outputDataType, mutableAggBufferOffset, inputAggBufferOffset) {

  override def update(buffer: SumLCCounter, input: InternalRow): SumLCCounter = {
    val evalCounter = deserialize(measure.eval(input).asInstanceOf[Array[Byte]])
    SumLCCounter.merge(buffer, evalCounter)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(measure)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)

}

object SumLCUtil extends Logging {

  def decodeToSumLCCounter(bytes: Array[Byte], codec: NullSafeValueSerializer): SumLCCounter = {
    if (bytes.nonEmpty) {
      val in = new KryoDataInput(new Input(bytes))
      val sumLC = codec.deserialize(in).asInstanceOf[Number]
      val timestamp = in.readLong()
      new SumLCCounter(sumLC, timestamp)
    } else {
      new SumLCCounter()
    }
  }

  def getNumericNullSafeSerializerByDataType(dataType: org.apache.spark.sql.types.DataType): NullSafeValueSerializer = {
    dataType match {
      case LongType => new LongSerializer
      case DoubleType => new DoubleSerializer
      case DecimalType() => new JavaBigDecimalSerializer
      case dt => throw new UnsupportedOperationException("Unsupported sum_lc dimension type: " + dt)
    }
  }

}
