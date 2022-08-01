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

import org.apache.kylin.measure.topn.TopNCounter
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class TopNUDAF(dataTp: org.apache.kylin.metadata.datatype.DataType, schema: StructType, isFirst: Boolean)
extends UserDefinedAggregateFunction {

  val counter = new TopNCounter[Seq[Any]](dataTp.getPrecision * TopNCounter.EXTRA_SPACE_RATE)

  override def inputSchema: StructType = if (isFirst) schema else bufferSchema

  override def bufferSchema: StructType = StructType(Seq(StructField("topN", dataType)))

  override def dataType: DataType = ArrayType(
    StructType(Seq(
      StructField("measure", DoubleType),
      StructField("dim", StructType(schema.fields.drop(1)))
    )))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    require(dataTp.toString.startsWith("topn"))
    buffer.update(0, null)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (isFirst) {
      merge(buffer, Row(Seq(Row(getMeasure(input), Row(getDims(input): _*)))))
    } else {
      merge(buffer, input)
    }
  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // input[0] can not be null cuz pos 0 is measure,
    // if value in measure is null, it will return 0, see getMeasure().
    require(!input.isNullAt(0))
    if (buffer.isNullAt(0)) {
      buffer.update(0, input.get(0))
    } else {
      counter.reset()
      // digest buffer
      digest(buffer, counter)

      // digest input row
      digest(input, counter)

      counter.sortAndRetain()
      val digested = counter.getCounterList.asScala.map(entry =>
        Row(entry.getCount, Row(entry.getItem: _*))
      )
      buffer.update(0, digested)
    }
  }

  override def evaluate(buffer: Row): Any = {
    if (!buffer.isNullAt(0)) {
      counter.reset()
      digest(buffer, counter)

      val seq = counter.getCounterList.asScala.map(
        entry => Row(entry.getCount, Row(entry.getItem: _*))
      )
      seq
    } else {
      null
    }
  }

  private def getDims(row: Row): Seq[Any] = {
    row.toSeq.drop(1)
  }

  private def getMeasure(row: Row): java.lang.Double = {
    val v = row.get(0)
    if (v != null) {
      v.toString.toDouble
    } else {
      null
    }
  }

  private def digest(row: Row, counter: TopNCounter[Seq[Any]]): Unit = {
    row.getSeq[Row](0).foreach(entry =>
      counter.offer(entry.getStruct(1).toSeq, getMeasure(entry))
    )
  }
}
