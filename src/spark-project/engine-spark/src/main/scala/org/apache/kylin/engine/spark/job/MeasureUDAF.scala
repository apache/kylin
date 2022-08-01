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

package org.apache.kylin.engine.spark.job

import java.nio.{BufferOverflowException, ByteBuffer}

import org.apache.kylin.measure.{MeasureAggregator, MeasureIngester, MeasureTypeFactory}
import org.apache.kylin.metadata.datatype.{DataTypeSerializer, DataType => KyDataType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

sealed abstract class MeasureUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = initInputSchema

  override def bufferSchema: StructType = initBufferSchema

  override def deterministic: Boolean = true

  override def dataType: DataType = initDataType

  protected var measureAggregator: MeasureAggregator[Any] = initMeasureAgg

  protected var serializer: DataTypeSerializer[Any] = initSer

  protected var byteBuffer: ByteBuffer = _

  protected var measureIngester: MeasureIngester[Any] = initMeasureIngester

  protected var encoder: MeasureEncoder[Any, Any] = _

  protected var isFirst: Boolean = _

  protected var dataTpName: String = _

  def initInputSchema: StructType

  def initBufferSchema: StructType

  def initDataType: DataType

  def initMeasureAgg: MeasureAggregator[Any]

  def initSer: DataTypeSerializer[Any]

  def initOutPutDataType: DataType

  def initMeasureIngester: MeasureIngester[Any]


  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    if (byteBuffer == null) {
      byteBuffer = ByteBuffer.allocate(serializer.maxLength())
      val dataType = KyDataType.getType(dataTpName)
      dataTpName match {
        case tp if tp.startsWith("hllc") =>
          encoder = new HLLCCountEnc(dataType).asInstanceOf[MeasureEncoder[Any, Any]]
        case "bitmap" =>
          encoder = new BitmapCountEnc(dataType).asInstanceOf[MeasureEncoder[Any, Any]]
        case tp if tp.startsWith("percentile") =>
          encoder = new PercentileCountEnc(dataType).asInstanceOf[MeasureEncoder[Any, Any]]
      }
    }
    buffer.update(0, null)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    byteBuffer.clear()
    try {
      var inputValue: Any = null
      if (isFirst) {
        inputValue = encoder.encoder(input.apply(0))
      } else {
        inputValue = serializer.deserialize(ByteBuffer.wrap(input.apply(0).asInstanceOf[Array[Byte]]))
      }
      if (buffer.isNullAt(0)) {
        serializer.serialize(inputValue, byteBuffer)
        buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
      } else {
        measureAggregator.reset()
        val bufferValue = serializer.deserialize(ByteBuffer.wrap(buffer.apply(0).asInstanceOf[Array[Byte]]))
        measureAggregator.aggregate(bufferValue)
        measureAggregator.aggregate(inputValue)
        serializer.serialize(measureAggregator.getState, byteBuffer)
        buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
      }
    } catch {
      case th: BufferOverflowException =>
        byteBuffer = ByteBuffer.allocate(byteBuffer.array().length * 2)
        update(buffer, input)
      case th: Throwable => throw th
    }
  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    try {
      if (!input.isNullAt(0)) {
        byteBuffer.clear()
        var inputValue = serializer.deserialize(ByteBuffer.wrap(input.apply(0).asInstanceOf[Array[Byte]]))
        if (buffer.isNullAt(0)) {
          serializer.serialize(inputValue, byteBuffer)
          buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
        } else {
          measureAggregator.reset()
          val bufferValue = serializer.deserialize(ByteBuffer.wrap(buffer.apply(0).asInstanceOf[Array[Byte]]))
          measureAggregator.aggregate(bufferValue)
          measureAggregator.aggregate(inputValue)
          serializer.serialize(measureAggregator.getState, byteBuffer)
          buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
        }
      }
    } catch {
      case th: BufferOverflowException =>
        byteBuffer = ByteBuffer.allocate(byteBuffer.array().length * 2)
        merge(buffer, input)
      case th: Throwable => throw th
    }
  }

  override def evaluate(buffer: Row): Any = {
    buffer.apply(0)
  }
}


class FirstUDAF(expression: String, dataTp: KyDataType, isfi: Boolean) extends MeasureUDAF {

  isFirst = isfi
  dataTpName = dataTp.toString

  override def initInputSchema: StructType = {
    if (isFirst) {
      StructType(Seq(StructField("input", StringType)))
    } else {
      StructType(Seq(StructField("input", BinaryType)))
    }
  }

  override def initBufferSchema: StructType = {
    StructType(Seq(StructField("init", BinaryType)))
  }

  override def initDataType: DataType = {
    BinaryType
  }

  override def initMeasureAgg: MeasureAggregator[Any] = {
    MeasureAggregator
      .create(if (expression == "$SUM0") "COUNT" else expression, dataTp).asInstanceOf[MeasureAggregator[Any]]
  }

  override def initSer: DataTypeSerializer[Any] = {
    val ser = DataTypeSerializer.create(dataTp).asInstanceOf[DataTypeSerializer[Any]]
    ser
  }

  override def initOutPutDataType: DataType = {
    BinaryType
  }

  override def initMeasureIngester: MeasureIngester[Any] = {
    MeasureTypeFactory.create(expression, dataTp).newIngester().asInstanceOf[MeasureIngester[Any]]
  }

}







