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

import org.apache.kylin.common.util.ByteBufferBackedInputStream

import java.io.DataInputStream
import java.nio.ByteBuffer
import java.util
import java.util.{Map => JMap}
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

  def result(filterSize: Int): Roaring64NavigableMap = {
    if (_map.size() != filterSize || _map.size() == 0) {
      new Roaring64NavigableMap()
    } else {
      val bitmap = _map.asScala.values.reduce { (a, b) =>
        a.and(b)
        a
      }
      bitmap
    }
  }
}
