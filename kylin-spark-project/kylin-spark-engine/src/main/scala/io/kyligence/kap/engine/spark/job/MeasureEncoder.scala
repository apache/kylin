/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package io.kyligence.kap.engine.spark.job

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