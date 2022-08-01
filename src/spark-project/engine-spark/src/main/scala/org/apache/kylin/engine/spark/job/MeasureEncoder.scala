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

import org.apache.kylin.measure.bitmap.{RoaringBitmapCounter, RoaringBitmapCounterFactory}
import org.apache.kylin.measure.hllc.HLLCounter
import org.apache.kylin.measure.percentile.PercentileCounter
import org.apache.kylin.metadata.datatype.DataType

sealed abstract class MeasureEncoder[T, V](dataType: DataType) {


  def encoder(value: T): V
}

class PercentileCountEnc(dataType: DataType) extends MeasureEncoder[Any, PercentileCounter](dataType: DataType) with Serializable {
  def this() = this(DataType.getType("String"))

  override def encoder(value: Any): PercentileCounter = {
    val current: PercentileCounter = new PercentileCounter(dataType.getPrecision)
    if (value != null) {
      current.add(value.toString.toDouble)
    }
    current
  }
}

class HLLCCountEnc(dataType: DataType) extends MeasureEncoder[Any, HLLCounter](dataType: DataType) with Serializable {
  def this() = this(DataType.getType("String"))

  //  TODO: support more args
  override def encoder(value: Any): HLLCounter = {
    val current: HLLCounter = new HLLCounter(dataType.getPrecision)
    if (value != null) {
      current.add(value.toString)
    }
    current
  }
}

class BitmapCountEnc(dataType: DataType) extends MeasureEncoder[Any, RoaringBitmapCounter](dataType: DataType) with Serializable {
  def this() = this(DataType.getType("String"))

  //  TODO: support more args
  override def encoder(value: Any): RoaringBitmapCounter = {
    val factory = RoaringBitmapCounterFactory.INSTANCE
    val bitmapCounter: RoaringBitmapCounter = factory.newBitmap.asInstanceOf[RoaringBitmapCounter]
    val current: RoaringBitmapCounter = bitmapCounter
    if (value != null) {
      current.add(java.lang.Long.parseLong(value.toString))
    }
    current
  }
}
