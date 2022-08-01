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

import org.apache.kylin.measure.percentile.PercentileCounter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{BinaryType, DataType, Decimal, DoubleType}

import java.nio.{BufferOverflowException, ByteBuffer}
import scala.annotation.tailrec

case class Percentile(aggColumn: Expression,
                      precision: Int,
                      quantile: Option[Expression] = None,
                      outputType: DataType = BinaryType,
                      mutableAggBufferOffset: Int = 0,
                      inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[PercentileCounter] with Serializable with Logging {

  override def children: Seq[Expression] = quantile match {
    case None => aggColumn :: Nil
    case Some(q) => aggColumn :: q :: Nil
  }

  override def nullable: Boolean = false

  override def createAggregationBuffer(): PercentileCounter = new PercentileCounter(precision)

  override def serialize(buffer: PercentileCounter): Array[Byte] = {
    serialize(buffer, new Array[Byte](1024 * 1024))
  }

  @tailrec
  private def serialize(buffer: PercentileCounter, bytes: Array[Byte]): Array[Byte] = {
    try {
      val output = ByteBuffer.wrap(bytes)
      buffer.writeRegisters(output)
      output.array().slice(0, output.position())
    } catch {
      case _: BufferOverflowException =>
        serialize(buffer, new Array[Byte](bytes.length * 2))
      case e =>
        throw e
    }
  }

  override def deserialize(bytes: Array[Byte]): PercentileCounter = {
    val counter = new PercentileCounter(precision)
    if (!bytes.isEmpty) {
      counter.readRegisters(ByteBuffer.wrap(bytes))
    }
    counter
  }


  override def merge(buffer: PercentileCounter, input: PercentileCounter): PercentileCounter = {
    buffer.merge(input)
    buffer
  }

  override def prettyName: String = "percentile"

  override def dataType: DataType = outputType

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def update(buffer: PercentileCounter, input: InternalRow): PercentileCounter = {
    val colValue = aggColumn.eval(input)
    colValue match {
      case d: Number =>
        buffer.add(d.doubleValue())
      case array: Array[Byte] =>
        buffer.merge(deserialize(array))
      case d: Decimal =>
        buffer.add(d.toDouble)
      case _ =>
        logDebug(s"unknown value $colValue")
    }
    buffer
  }

  override def eval(buffer: PercentileCounter): Any = {
    outputType match {
      case BinaryType =>
        serialize(buffer)
      case DoubleType =>
        val counter = quantile match {
          case Some(Literal(value, _)) =>
            val evalQuantile = value match {
              case d: Decimal => d.toDouble
              case i: Integer => i.toDouble
              case _ => -1
            }
            val counter2 = new PercentileCounter(buffer.getCompression, evalQuantile)
            counter2.merge(buffer)
            counter2
          case None => buffer
        }
        counter.getResultEstimate
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

