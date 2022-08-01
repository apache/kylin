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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


// scalastyle:off
/**
  *
  * <br>Returns the only one  value of `expr` for a group of rows.
  * <br>If the  value of `expr` is `null`, it returns `null` (respecting nulls).
  * <br>If more than one row in this `expr`, it will throw RuntimeException
  *
  * <pre>for example:
  * val structField = dataFrame.schema.head
  * SingleValueAgg(structField).apply(Column(expr))
  * </pre>
  *
  * @return only one row  or null
  * @param structField [[org.apache.spark.sql.types.StructField]]
  * @throws RuntimeException when more than one row in this `expr`
  */
case class SingleValueAgg(structField: StructField) extends UserDefinedAggregateFunction {


  override def inputSchema = StructType(Seq(
    StructField("value_input", structField.dataType)))

  override def bufferSchema = StructType(Seq(
    StructField("value_buffer", structField.dataType),
    StructField("value_count", IntegerType)))


  override def dataType: DataType = structField.dataType


  override def deterministic = true

  //filed pos for bufferSchema
  val VALUE_POS = 0
  val COUNTER_POS = 1

  //max count
  val COUNTER_THRESHOLD = 1

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(VALUE_POS, null)
    buffer.update(COUNTER_POS, 0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(COUNTER_POS, buffer.getInt(COUNTER_POS) + 1)
    checkBufferCount(buffer)

    buffer.update(VALUE_POS, input.apply(0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(COUNTER_POS, buffer1.getInt(COUNTER_POS) + buffer2.getInt(COUNTER_POS))
    checkBufferCount(buffer1)

    buffer1.update(VALUE_POS, buffer2.apply(VALUE_POS))
  }

  override def evaluate(buffer: Row) = buffer.apply(VALUE_POS)


  private def checkBufferCount(buffer: MutableAggregationBuffer): Unit = {
    if (buffer.getInt(COUNTER_POS) > COUNTER_THRESHOLD) {
      throw new RuntimeException(s"""more than $COUNTER_THRESHOLD row returned in a single value aggregation""")
    }
  }
}
