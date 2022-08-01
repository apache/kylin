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

package org.apache.spark.sql.udf

import java.nio.ByteBuffer
import java.util

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
import org.apache.spark.sql.util.SparderTypeUtil

class SparderAggFun(funcName: String, dataTp: KyDataType)
  extends UserDefinedAggregateFunction
    with Logging {

  protected val _inputDataType = {
    var schema = StructType(Seq(StructField("inputBinary", BinaryType)))
    if (funcName.toLowerCase.startsWith("percentile")) {
      schema.add("argc", DoubleType)
    } else {
      schema
    }
  }

  protected val _bufferSchema: StructType = {
    var schema = StructType(Seq(StructField("bufferBinary", BinaryType)))
    if (funcName.toLowerCase.startsWith("percentile")) {
      schema.add("argc", DoubleType)
    } else {
      schema
    }
  }

  protected val _returnDataType: DataType =
    SparderTypeUtil.kylinTypeToSparkResultType(dataTp)

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
