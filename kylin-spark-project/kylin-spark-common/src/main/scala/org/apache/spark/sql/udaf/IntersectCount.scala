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

package org.apache.spark.sql.udaf

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.{Input, KryoDataInput, KryoDataOutput, Output}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{DataType, LongType}
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.JavaConverters._

@SerialVersionUID(1)
case class IntersectCount(child1: Expression, child2: Expression, child3: Expression, mutableAggBufferOffset: Int = 0,
                          inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[IntersectBitmapCounter] with Serializable with Logging {

  private var filters: Map[Any, String] = _

  override def createAggregationBuffer(): IntersectBitmapCounter = new IntersectBitmapCounter()

  override def update(counter: IntersectBitmapCounter, input: InternalRow): IntersectBitmapCounter = {
    if (filters == null) {
      filters = child3.eval(input).asInstanceOf[GenericArrayData].array.map(filter => filter -> filter.toString).toMap
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
    counter.result(child3.asInstanceOf[Literal].value.asInstanceOf[GenericArrayData].array.distinct.length)
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

  override def dataType: DataType = LongType

  override def children: Seq[Expression] = child1 :: child2 :: child3 :: Nil
}
