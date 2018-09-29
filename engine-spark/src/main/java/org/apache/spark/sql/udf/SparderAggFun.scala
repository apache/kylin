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

import org.apache.kylin.gridtable.GTInfo
import org.apache.kylin.measure.MeasureAggregator
import org.apache.kylin.metadata.datatype.DataTypeSerializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil

class SparderAggFun(funcName: String, dataTp: org.apache.kylin.metadata.datatype.DataType)
  extends UserDefinedAggregateFunction
    with Logging {

  protected val _inputDataType = {
    val schema = StructType(Seq(StructField("inputBinary", BinaryType)))
    schema
  }

  protected val _bufferSchema: StructType = {
    val schema = StructType(Seq(StructField("bufferBinary", BinaryType)))
    schema
  }

  protected val _returnDataType: DataType =
    SparderTypeUtil.kylinTypeToSparkResultType(dataTp)

  protected var byteBuffer: ByteBuffer = null
  protected var init = false
  protected var gtInfo: GTInfo = _
  protected var measureAggregator: MeasureAggregator[Any] = _
  protected var colId: Int = _
  protected var serializer: DataTypeSerializer[Any] = _
  protected var aggregator: MeasureAggregator[Any] = _

  var time = System.currentTimeMillis()

  override def bufferSchema: StructType = _bufferSchema

  override def inputSchema: StructType = _inputDataType

  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val isSum0 = (funcName == "$SUM0")
    if (byteBuffer == null) {
      serializer = DataTypeSerializer.create(dataTp).asInstanceOf[DataTypeSerializer[Any]]
      byteBuffer = ByteBuffer.allocate(serializer.maxLength)
    }

    aggregator = MeasureAggregator
      .create(if (isSum0) "COUNT" else funcName, dataTp)
      .asInstanceOf[MeasureAggregator[Any]]

    aggregator.reset()

    val initVal = if (isSum0) {
      // $SUM0 is the rewritten form of COUNT, which should return 0 instead of null in case of no input
      byteBuffer.clear()
      serializer.serialize(aggregator.getState, byteBuffer)
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
      try {
        val byteArray = input.apply(0).asInstanceOf[Array[Byte]]
        if (byteArray.length == 0) {
          return
        }
        val oldValue = if(buffer.isNullAt(0)) null else serializer.deserialize(ByteBuffer.wrap(buffer.apply(0).asInstanceOf[Array[Byte]]))
        val newValue = serializer.deserialize(ByteBuffer.wrap(byteArray))

        val aggedValue = aggregator.aggregate(oldValue, newValue)

        if (aggedValue != null) {
          byteBuffer.clear()
          serializer.serialize(aggedValue, byteBuffer)
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
      null
    } else {
      val ret = dataTp.getName match {
        case dt if dt.startsWith("percentile") => buffer.apply(0).asInstanceOf[Array[Byte]]
        case "hllc" => buffer.apply(0).asInstanceOf[Array[Byte]]
        case "bitmap" => buffer.apply(0).asInstanceOf[Array[Byte]]
        case "dim_dc" => buffer.apply(0).asInstanceOf[Array[Byte]]
        case "extendedcolumn" => buffer.apply(0).asInstanceOf[Array[Byte]]
        case "raw" => buffer.apply(0).asInstanceOf[Array[Byte]]
        case t if t startsWith "top" => buffer.apply(0).asInstanceOf[Array[Byte]]
        case _ => null
      }

      if (ret != null)
        ret
      else
        throw new IllegalArgumentException("unsupported function")
    }
  }

  override def toString: String = {
    s"SparderAggFun@$funcName${dataType.toString}"
  }

  override def dataType: DataType = _returnDataType
}
