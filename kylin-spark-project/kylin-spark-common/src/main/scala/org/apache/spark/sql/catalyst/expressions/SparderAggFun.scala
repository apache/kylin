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

package org.apache.spark.sql.catalyst.expressions

import java.nio.ByteBuffer
import java.util
import java.util.Locale

import com.google.common.collect.Maps
import org.apache.kylin.measure.MeasureAggregator
import org.apache.kylin.measure.bitmap.BitmapCounter
import org.apache.kylin.measure.dim.DimCountDistinctCounter
import org.apache.kylin.measure.hllc.HLLCounter
import org.apache.kylin.measure.percentile.PercentileCounter
import org.apache.kylin.metadata.datatype.{DataTypeSerializer, DataType => KyDataType}
import org.apache.kylin.metadata.model.FunctionDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkTypeUtil

class SparderAggFun(funcName: String, dataTp: KyDataType)
  extends UserDefinedAggregateFunction
    with Logging {

  protected val _inputDataType = {
    var schema = StructType(Seq(StructField("inputBinary", BinaryType)))
    if (funcName.toLowerCase(Locale.ROOT).startsWith("percentile")) {
      schema.add("argc", DoubleType)
    } else {
      schema
    }
  }

  protected val _bufferSchema: StructType = {
    var schema = StructType(Seq(StructField("bufferBinary", BinaryType)))
    if (funcName.toLowerCase(Locale.ROOT).startsWith("percentile")) {
      schema.add("argc", DoubleType)
    } else {
      schema
    }
  }

  protected val _returnDataType: DataType =
    SparkTypeUtil.kylinTypeToSparkResultType(dataTp)

  protected var byteBuffer: ByteBuffer = null
  protected var init = false
  protected var measureAggregator: MeasureAggregator[Any] = _
  protected var colId: Int = _
  protected var serializer: DataTypeSerializer[Any] = _

  // scalastyle:off
  protected var measureAggregatorMap: util.HashMap[String, MeasureAggregator[Any]] = Maps.newHashMap()

  var time = System.currentTimeMillis()

  override def bufferSchema: StructType = _bufferSchema

  override def inputSchema: StructType = _inputDataType

  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val isCount = (funcName == FunctionDesc.FUNC_COUNT)

    if (byteBuffer == null) {
      measureAggregator = MeasureAggregator
        .create(funcName, dataTp)
        .asInstanceOf[MeasureAggregator[Any]]
      serializer = DataTypeSerializer.create(dataTp).asInstanceOf[DataTypeSerializer[Any]]
      byteBuffer = ByteBuffer.allocate(1024 * 1024)
    }

    val initVal = if (isCount) {
      // return 0 instead of null in case of no input
      measureAggregator.reset()
      byteBuffer.clear()
      serializer.serialize(measureAggregator.getState, byteBuffer)
      byteBuffer.array().slice(0, byteBuffer.position())
    } else {
      null
    }
    buffer.update(0, initVal)
  }

  val MAX_BUFFER_CAP: Int = 50 * 1024 * 1024

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    merge(buffer, input)
  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      measureAggregator.reset()
      byteBuffer.clear()
      try {
        val byteArray = input.apply(0).asInstanceOf[Array[Byte]]
        if (byteArray.length == 0) {
          return
        }
        if (buffer.isNullAt(0)) {
          buffer.update(0, byteArray)
          if (inputSchema.length > 1) {
            if (!input.isNullAt(1)) {
              buffer.update(1, input.get(1))
            }
          }
        } else {
          val newValue = serializer.deserialize(ByteBuffer.wrap(byteArray))
          measureAggregator.aggregate(newValue)
          val bytes = buffer.apply(0).asInstanceOf[Array[Byte]]
          val oldValue = serializer.deserialize(ByteBuffer.wrap(bytes))
          measureAggregator.aggregate(oldValue)
          val aggregatored = measureAggregator.getState
          serializer.serialize(aggregatored, byteBuffer)
          buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
        }
      } catch {
        case e: Exception =>
          throw new Exception(
            "error data is: " + input
              .apply(0)
              .asInstanceOf[Array[Byte]]
              .mkString(","),
            e)
      }
    }
  }

  override def evaluate(buffer: Row): Any = {
    if (buffer.isNullAt(0)) {
      // If the buffer value is still null, we return null.
      null

    } else {
      // Otherwise, the intermediate sum is the final result.

      val x = serializer.deserialize(ByteBuffer.wrap(buffer.apply(0).asInstanceOf[Array[Byte]]))

      //scalastyle:off
      val ret = dataTp.getName match {
        case dt if dt.startsWith("percentile") =>
          val counter = x.asInstanceOf[PercentileCounter]
          val counter2 = new PercentileCounter(counter.getCompression, buffer.getDouble(1))
          counter2.merge(counter)
          counter2.getResultEstimate
        case "hllc" => x.asInstanceOf[HLLCounter].getCountEstimate
        case "bitmap" => x.asInstanceOf[BitmapCounter].getCount
        case "dim_dc" => x.asInstanceOf[DimCountDistinctCounter].result()
        case _ => null
      }

      ret
    }
  }

  override def toString: String = {
    s"SparderAggFun@$funcName${dataType.toString}"
  }

  override def dataType: DataType = _returnDataType
}
