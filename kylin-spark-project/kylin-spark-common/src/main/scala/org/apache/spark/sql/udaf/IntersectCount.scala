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
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.JavaConverters._

@SerialVersionUID(1)
case class IntersectCount(child1: Expression, child2: Expression, child3: Expression,
                          returnDataType: DataType, upperBound: Int = 10000000,
                          mutableAggBufferOffset: Int = 0,
                          inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[IntersectBitmapCounter] with Serializable with Logging {

  private var filters: Map[Any, String] = _

  override def createAggregationBuffer(): IntersectBitmapCounter = new IntersectBitmapCounter()

  override def update(counter: IntersectBitmapCounter, input: InternalRow): IntersectBitmapCounter = {
    if (filters == null) {
      filters = child3.eval(input).asInstanceOf[GenericArrayData]
        .array.map(filter => filter -> filter.toString).toMap
    }
    val bitmap = child1.eval(input).asInstanceOf[Array[Byte]]
    val key = child2.eval(input)
    if (bitmap != null && key != null && filters.contains(key)) {
      counter.add(filters(key), bitmap)
    }
    counter
  }

  override def merge(counter: IntersectBitmapCounter, input: IntersectBitmapCounter): IntersectBitmapCounter = {
    if (input != null) {
      counter.merge(input)
    }
    counter
  }

  override def eval(counter: IntersectBitmapCounter): Any = {
    val map = counter.result(
      child3.asInstanceOf[Literal].value.asInstanceOf[GenericArrayData].array.distinct.length)
    dataType match {
      // for intersect_count
      case LongType => map.getLongCardinality
      // for intersect_value
      case StringType =>
        val intCardinality = map.getIntCardinality
        if (intCardinality > upperBound) {
          throw new UnsupportedOperationException(s"Cardinality of the bitmap is greater than " +
            s"configured upper bound(${upperBound})")
        }
        val result = new StringBuffer("")
        if (intCardinality > 0) {
          result.append("[").append(StringUtils.join(map.iterator(), ",")).append("]");
        }
        UTF8String.fromString(result.toString)
      case ArrayType(LongType, false) =>
        val cardinality = map.getIntCardinality
        if (cardinality > upperBound) {
          throw new UnsupportedOperationException(s"Cardinality of the bitmap is greater than " +
            s"configured upper bound(${upperBound})")
        }
        val longs = new Array[Long](cardinality)
        var id = 0
        val iterator = map.iterator()
        while (iterator.hasNext) {
          longs(id) = iterator.next()
          id += 1
        }
        new GenericArrayData(longs)
    }
  }

  var array: Array[Byte] = _
  var output: Output = _

  override def serialize(counter: IntersectBitmapCounter): Array[Byte] = {
    try {
      if (counter != null) {
        if (array == null) {
          array = new Array[Byte](1024 * 1024)
          output = new Output(array)
        }
        output.clear()
        val kryo = new KryoDataOutput(output)
        val map = counter.map()
        kryo.writeInt(map.size)
        map.entrySet().asScala.foreach { entry =>
          kryo.writeUTF(entry.getKey)
          entry.getValue.serialize(kryo)
        }
        val i = output.position()
        output.close()
        array.slice(0, i)
      } else {
        Array.empty[Byte]
      }
    } catch {
      case th: KryoException if th.getMessage.contains("Buffer overflow") =>
        logInfo(s"Resize buffer size to ${array.length * 2}")
        array = new Array[Byte](array.length * 2)
        output.setBuffer(array)
        serialize(counter)
      case th =>
        throw th
    }
  }


  override def deserialize(bytes: Array[Byte]): IntersectBitmapCounter = {
    val intersectBitmapCounter = new IntersectBitmapCounter
    if (bytes.nonEmpty) {
      val input = new KryoDataInput(new Input(bytes))
      val size = input.readInt()
      if (size != 0) {
        val map = intersectBitmapCounter.map()
        for (_: Int <- 1 to size) {
          val key = input.readUTF()
          val bitmap = new Roaring64NavigableMap()
          bitmap.deserialize(input)
          map.put(key, bitmap)
        }
      }
    }
    intersectBitmapCounter
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = returnDataType

  override def children: Seq[Expression] = child1 :: child2 :: child3 :: Nil
}
