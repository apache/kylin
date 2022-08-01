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

package org.apache.spark.sql.udf

import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.udaf.BitmapSerAndDeSer
import org.roaringbitmap.longlong.Roaring64NavigableMap

object SubtractBitmapImpl {
  def evaluate2Bytes(map1: Array[Byte], map2: Array[Byte]): Array[Byte] = {
    val map = evaluate2Bitmap(map1, map2)
    if (map == null) {
      null
    } else {
      BitmapSerAndDeSer.get().serialize(map)
    }
  }

  @inline
  def evaluate2Bitmap(map1: Array[Byte], map2: Array[Byte]): Roaring64NavigableMap = {
    if (map1 == null) {
      null
    } else if (map2 == null) {
      BitmapSerAndDeSer.get().deserialize(map1)
    } else {
      val resultMap = BitmapSerAndDeSer.get().deserialize(map1)
      resultMap.andNot(BitmapSerAndDeSer.get().deserialize(map2))
      resultMap
    }
  }

  def evaluate2Values(map1: Array[Byte], map2: Array[Byte], bitmapUpperBound: Int): GenericArrayData = {
    val map = evaluate2Bitmap(map1, map2)
    if (map == null) {
      null
    } else {
      val cardinality = map.getIntCardinality
      if (cardinality > bitmapUpperBound) {
        throw new UnsupportedOperationException(s"Cardinality of the bitmap is greater than configured upper bound($bitmapUpperBound).")
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
}
