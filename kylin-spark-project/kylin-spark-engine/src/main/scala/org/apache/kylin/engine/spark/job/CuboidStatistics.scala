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


import org.apache.kylin.engine.spark.metadata.SegmentInfo
import org.apache.kylin.measure.hllc.HLLCounter
import org.apache.kylin.shaded.com.google.common.hash.{HashFunction, Hashing}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable


object CuboidStatistics {

  def sample(inputDs: Dataset[Row], seg: SegmentInfo): Array[(String, AggInfo)] = {
    seg.getAllLayout.map(x => x.getId)
    val rkc = seg.allColumns.count(c => c.rowKey)
    val res = inputDs.rdd //.sample(withReplacement = false, 0.3)
      .mapPartitions(new CuboidStatistics(seg.getAllLayout.map(x => x.getId), rkc).statisticsInPartition)
    val l = res.map(a => (a.key, a)).reduceByKey((a, b) => a.merge(b)).collect()
    l.foreach(x => println(x._1 + " >>><<< " + x._2.cuboid.counter.getCountEstimate))
    l
  }
}

class CuboidStatistics(ids: List[Long], rkc: Int) extends Serializable {
  private val info = mutable.Map[String, AggInfo]()
  val allCuboidsBitSet: Array[Array[Integer]] = getCuboidBitSet(ids, rkc)
  private val hf: HashFunction = Hashing.murmur3_128
  private val rowHashCodesLong = new Array[Long](rkc)

  def statisticsInPartition(rows: Iterator[Row]): Iterator[AggInfo] = {
    init()
    rows.foreach(update)
    info.valuesIterator
  }

  def init(): Unit = {
    ids.foreach(i => info.put(i.toString, AggInfo(i.toString)))
  }

  def update(r: Row): Unit = {
    println(r)
    updateCuboid(r)
  }

  def updateCuboid(r: Row): Unit = {
    // generate hash for each row key column

    var idx = 0
    while (idx < rkc) {
      val hc = hf.newHasher
      var colValue = r.get(idx).toString
      if (colValue == null) colValue = "0"
      //add column ordinal to the hash value to distinguish between (a,b) and (b,a)
      rowHashCodesLong(idx) = hc.putUnencodedChars(colValue).hash().padToLong() + idx
      idx += 1
    }

    // use the row key column hash to get a consolidated hash for each cuboid
    val n = allCuboidsBitSet.length
    idx = 0
    while (idx < n) {
      var value: Long = 0
      var position = 0
      while (position < allCuboidsBitSet(idx).length) {
        value += rowHashCodesLong(allCuboidsBitSet(idx)(position))
        position += 1
      }
      info(ids(idx).toString).cuboid.counter.addHashDirectly(value)
      idx += 1
    }
  }

  def getCuboidBitSet(cuboidIds: List[Long], nRowKey: Int): Array[Array[Integer]] = {
    val allCuboidsBitSet: Array[Array[Integer]] = new Array[Array[Integer]](cuboidIds.length)
    var j: Int = 0
    while (j < cuboidIds.length) {
      val cuboidId: Long = cuboidIds(j)
      allCuboidsBitSet(j) = new Array[Integer](java.lang.Long.bitCount(cuboidId))
      var mask: Long = 1L << (nRowKey - 1)
      var position: Int = 0
      var i: Int = 0
      while (i < nRowKey) {
        if ((mask & cuboidId) > 0) {
          allCuboidsBitSet(j)(position) = i
          position += 1
        }
        mask = mask >> 1
        i += 1
      }
      j += 1
    }
    allCuboidsBitSet
  }
}

case class AggInfo(key: String,
                   cuboid: CuboidInfo = CuboidInfo(new HLLCounter()),
                   sample: SampleInfo = SampleInfo(),
                   dimension: DimensionInfo = DimensionInfo()) {
  def merge(o: AggInfo): AggInfo = {
    this.cuboid.counter.merge(o.cuboid.counter)
    this
  }
}

case class CuboidInfo(counter: HLLCounter = new HLLCounter(14))

case class SampleInfo(data: Array[String] = new Array(3))

case class DimensionInfo(range: mutable.Map[String, String] = mutable.Map[String, String]())
