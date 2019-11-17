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

import java.io.DataInputStream
import java.nio.ByteBuffer
import java.util
import java.util.{Map => JMap}

import org.apache.kylin.common.util.ByteBufferBackedInputStream
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.JavaConverters._

class IntersectBitmapCounter() {
  val _map: JMap[String, Roaring64NavigableMap] = new util.LinkedHashMap[String, Roaring64NavigableMap]()

  def map(): JMap[String, Roaring64NavigableMap] = _map

  def merge(other: IntersectBitmapCounter): Unit = {
    val otherMap = other.map()
    otherMap.entrySet().asScala.foreach { entry =>
      val key = entry.getKey
      val value = entry.getValue
      if (_map.containsKey(key)) {
        _map.get(key).or(value)
      } else {
        _map.put(key, value)
      }
    }
  }

  def add(key: String, value: Array[Byte]): Unit = {
    val newBitmap = new Roaring64NavigableMap()
    if (value.nonEmpty) {
      val bbi = new ByteBufferBackedInputStream(ByteBuffer.wrap(value))
      newBitmap.deserialize(new DataInputStream(bbi))
    }
    if (_map.containsKey(key)) {
      _map.get(key).or(newBitmap)
    } else {
      _map.put(key, newBitmap)
    }
  }

  def result(filterSize: Int): Long = {
    if (_map.size() != filterSize || _map.size() == 0) {
      0
    } else {
      val bitmap = _map.asScala.values.reduce { (a, b) =>
        a.and(b)
        a
      }
      bitmap.getLongCardinality
    }
  }
}
