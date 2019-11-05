/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package io.kyligence.kap.engine.spark.job

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







