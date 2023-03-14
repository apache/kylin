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
  private val INIT_ARRAY_BUFFER_LENGTH = 32;
  private val MAX_ARRAY_BUFFER_LENGTH = 256;

  lazy val serializer: NullSafeValueSerializer = SumLCUtil.getNumericNullSafeSerializerByDataType(wrapDataType)

  override def prettyName: String = "sum_lc"

  override def eval(buffer: SumLCCounter): Any = {
    outputDataType match {
      case BinaryType =>
        serialize(buffer)
      case DecimalType() =>
        if (buffer == null || buffer.getSumLC == null) return null;
        Decimal.fromDecimal(buffer.getSumLC.asInstanceOf[java.math.BigDecimal])
      case _ =>
        if (buffer == null) return null;
        buffer.getSumLC
    }
  }

  override def createAggregationBuffer(): SumLCCounter = null

  override def merge(buffer: SumLCCounter, input: SumLCCounter): SumLCCounter = {
    SumLCCounter.merge(buffer, input)
  }

  override def serialize(buffer: SumLCCounter): Array[Byte] = {
    if (buffer == null) {
      Array.empty[Byte]
    } else {
      val output: Output = new Output(INIT_ARRAY_BUFFER_LENGTH, MAX_ARRAY_BUFFER_LENGTH)
      try {
        val dataOutput = new KryoDataOutput(output)
        serializer.serialize(dataOutput, buffer.getSumLC)
        dataOutput.writeLong(buffer.getTimestamp)
        val mark = output.position()
        val bufferArray = output.getBuffer
        bufferArray.slice(0, mark)
      } catch {
        case th: Throwable =>
          throw th
      } finally if (output != null) {
        output.close()
      }
    }
  }

  override def deserialize(bytes: Array[Byte]): SumLCCounter = {
    SumLCUtil.decodeToSumLCCounter(bytes, serializer)
  }

  override def nullable: Boolean = true

  override def dataType: DataType = outputDataType

  protected def sumLCUpdateInternal(buffer: SumLCCounter, columnVal: Number, timestampVal: Long): SumLCCounter = {
    if (buffer == null) {
      new SumLCCounter(columnVal, timestampVal)
    } else {
      buffer.update(columnVal, timestampVal)
      buffer
    }
  }
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
    val dateEvalVal = dateCol.eval(input)
    if (dateEvalVal == null || dateEvalVal.toString.toUpperCase().equals("NULL")) {
      buffer
    } else {
      val dateValStr = String.valueOf(dateEvalVal).trim
      val timestampVal = DateFormat.stringToMillis(dateValStr)
      sumLCUpdateInternal(buffer, columnVal, timestampVal)
    }
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
    val valAndTsPair = SumLCUtil.decodeToValAndTs(measure.eval(input).asInstanceOf[Array[Byte]], serializer)
    if (valAndTsPair == null) {
      buffer
    } else {
      val columnVal = valAndTsPair._1
      val timestampVal = valAndTsPair._2
      sumLCUpdateInternal(buffer, columnVal, timestampVal)
    }
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

  def decodeToValAndTs(bytes: Array[Byte], codec: NullSafeValueSerializer): (Number, Long) = {
    if (bytes.isEmpty) {
      null
    } else {
      val in = new KryoDataInput(new Input(bytes))
      val sumLC = codec.deserialize(in).asInstanceOf[Number]
      val timestamp = in.readLong()
      (sumLC, timestamp)
    }
  }

  def decodeToSumLCCounter(bytes: Array[Byte], codec: NullSafeValueSerializer): SumLCCounter = {
    val valAndTsPair = decodeToValAndTs(bytes, codec)
    if (valAndTsPair == null) {
      null
    } else {
      new SumLCCounter(valAndTsPair._1, valAndTsPair._2)
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
