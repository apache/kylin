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

import java.util.regex.Pattern

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.{Input, KryoDataInput, KryoDataOutput, Output}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.JavaConverters._

object IntersectCount {
  val RAW_STRING: String = "RAWSTRING"
  val REG_EXP: String = "REGEXP"
}

@SerialVersionUID(1)
case class IntersectCount(child1: Expression, child2: Expression, child3: Expression,
                          filterType: Expression, dataType: DataType, separator: String,
                          bitmapUpperBound: Int = 1000000,
                          mutableAggBufferOffset: Int = 0,
                          inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[IntersectBitmapCounter] with Serializable with Logging {

  private var filter: IntersectFilter = _

  override def createAggregationBuffer(): IntersectBitmapCounter = new IntersectBitmapCounter()

  override def update(counter: IntersectBitmapCounter, input: InternalRow): IntersectBitmapCounter = {
    if (filter == null) {
      filter = (child2.dataType, filterType.eval(input).toString) match {
        case (StringType, IntersectCount.RAW_STRING) => RawStringFilter(separator)
        case (StringType, IntersectCount.REG_EXP) => RegexpFilter()
        case _ => GenericFilter()
      }
      filter.init(child3.eval(input).asInstanceOf[GenericArrayData].array)
    }
    val bitmap = child1.eval(input).asInstanceOf[Array[Byte]]
    val key = child2.eval(input)
    if (bitmap != null && key != null) {
      filter.matchResult(key).foreach(counter.add(_, bitmap))
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
    val map = counter.result(child3.asInstanceOf[Literal].value.asInstanceOf[GenericArrayData].array.distinct.length)
    dataType match {
      case LongType => map.getLongCardinality
      case ArrayType(LongType, false) =>
        val cardinality = map.getIntCardinality
        if (cardinality > bitmapUpperBound) {
          throw new UnsupportedOperationException(s"Cardinality of the bitmap is greater than configured upper bound($bitmapUpperBound).")
        }
        val longs = new Array[Long](cardinality)
        var id = 0
        val iterator = map.iterator()
        while(iterator.hasNext) {
          longs(id) = iterator.next()
          id += 1
        }
        new GenericArrayData(longs)
      case BinaryType => BitmapSerAndDeSer.get().serialize(map)
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
        logWarning(s"Resize buffer size to ${array.length * 2}")
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

  override def children: Seq[Expression] = child1 :: child2 :: child3 :: filterType :: Nil

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

sealed abstract class IntersectFilter {
  def init(filters: Array[Any]): Unit

  def matchResult(value: Any): Array[String]
}

case class RegexpFilter() extends IntersectFilter {

  private var filterTuples: Array[(Pattern, String)] = _

  override def init(filters: Array[Any]): Unit = {
    val rawStrings = filters.map(_.toString)
    val patterns = rawStrings.map(str => Pattern.compile(str))
    filterTuples = patterns.zip(rawStrings)
  }

  override def matchResult(value: Any): Array[String] = {
    val string = value.toString
    filterTuples.filter{
      case (pattern: Pattern, _) => pattern.matcher(string).matches()
    }.map(_._2)
  }
}

case class RawStringFilter(separator: String) extends IntersectFilter {

  private var filterTuples: Array[(Set[UTF8String], String)] = _

  override def init(filters: Array[Any]): Unit = {
    val rawStrings = filters.map(_.toString)
    val sets = rawStrings.map(_.split(separator).map(UTF8String.fromString).toSet)
    filterTuples = sets.zip(rawStrings)
  }

  override def matchResult(value: Any): Array[String] = {
    filterTuples.filter{
      case (sets: Set[UTF8String], _) => sets.contains(value.asInstanceOf[UTF8String])
    }.map(_._2)
  }
}

case class GenericFilter() extends IntersectFilter {
  private var filterMap: Map[Any, String] = _

  override def init(filters: Array[Any]): Unit = {
    filterMap = filters.map(filter => filter -> filter.toString).toMap
  }

  override def matchResult(value: Any): Array[String] = {
    if (filterMap.contains(value)) {
      Array(filterMap(value))
    } else {
      Array.empty
    }
  }
}
