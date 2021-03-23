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

/**
 * Calculate HLLCounter for each cuboid, to serve Cube Planner (to calculate cost and benefit of each cuboid).
 */
object CuboidStatisticsJob {

  /**
   * @param inputDs Part of FlatTable which contains all normal dimensions
   * @return Cuboid level statistics data
   */
  def statistics(inputDs: Dataset[Row], seg: SegmentInfo): Array[(Long, AggInfo)] = {

    val rkc = seg.allColumns.count(c => c.rowKey)
    // maybe we should use sample operation to reduce cost later
    val res = inputDs.rdd.repartition(inputDs.sparkSession.sparkContext.defaultParallelism)
      .mapPartitions(new CuboidStatisticsJob(seg.getAllLayout.map(x => x.getId).toArray, rkc).statisticsWithinPartition)
    val l = res.map(a => (a.key, a)).reduceByKey((a, b) => a.merge(b)).collect()
    //    l.foreach(x => println(x._1 + " >>><<< " + x._2.cuboid.counter.getCountEstimate))
    l
  }

  def statistics(inputDs: Dataset[Row], seg: SegmentInfo, layoutIds: Array[Long]): Array[(Long, AggInfo)] = {
    val res = inputDs.rdd.repartition(inputDs.sparkSession.sparkContext.defaultParallelism)
      .mapPartitions(new CuboidStatisticsJob(layoutIds, seg.allColumns.count(c => c.rowKey)).statisticsWithinPartition)
    val l = res.map(a => (a.key, a)).reduceByKey((a, b) => a.merge(b)).collect()
    l
  }


}

class CuboidStatisticsJob(ids: Array[Long], rkc: Int) extends Serializable {
  private val info = mutable.LongMap[AggInfo]()
  private var allCuboidsBitSet: Array[Array[Integer]] = Array()
  private val hf: HashFunction = Hashing.murmur3_128
  private val rowHashCodesLong = new Array[Long](rkc)
  private var idx = 0
  private var meter2 = 0L
  private var startMills = 0L
  private var endMills = 0L


  def statisticsWithinPartition(rows: Iterator[Row]): Iterator[AggInfo] = {
    init()
    println("CuboidStatisticsJob-statisticsWithinPartition1-" + System.currentTimeMillis())
    rows.foreach(update)
    printStat()
    println("CuboidStatisticsJob-statisticsWithinPartition2-" + System.currentTimeMillis())
    info.valuesIterator
  }

  def init(): Unit = {
    println("CuboidStatisticsJob-Init1-" + System.currentTimeMillis())
    allCuboidsBitSet = getCuboidBitSet(ids, rkc)
    ids.foreach(i => info.put(i, AggInfo(i)))
    println("CuboidStatisticsJob-Init2-" + System.currentTimeMillis())
  }

  def update(r: Row): Unit = {
    idx += 1
    if (idx <= 5)
      println(r)
    updateCuboid(r)
  }

  def updateCuboid(r: Row): Unit = {
    // generate hash for each row key column
    var idx = 0
    while (idx < rkc) {
      val hc = hf.newHasher
      val colValue = if (r.get(idx) == null) "0" else r.get(idx).toString
      // add column ordinal to the hash value to distinguish between (a,b) and (b,a)
      rowHashCodesLong(idx) = hc.putUnencodedChars(colValue).hash().padToLong() + idx
      idx += 1
    }

    startMills = System.currentTimeMillis()
    // use the row key column hash to get a consolidated hash for each cuboid
    val n = allCuboidsBitSet.length
    idx = 0
    while (idx < n) {
      var value: Long = 0
      var position = 0
      val currCuboidBitSet = allCuboidsBitSet(idx)
      val currCuboidLength = currCuboidBitSet.length
      while (position < currCuboidLength) {
        value += rowHashCodesLong(currCuboidBitSet(position))
        position += 1
      }
      info(ids(idx)).cuboid.counter.addHashDirectly(value)
      idx += 1
    }
    endMills = System.currentTimeMillis()
    meter2 += (endMills - startMills)
  }

  def getCuboidBitSet(cuboidIds: Array[Long], nRowKey: Int): Array[Array[Integer]] = {
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

  def printStat(): Unit = {
    println("    Stats")
    println("i      :" + idx)
    println("meter  :" + meter2)
  }
}

case class AggInfo(key: Long,
                   cuboid: CuboidInfo = CuboidInfo(new HLLCounter()),
                   sample: SampleInfo = SampleInfo(),
                   dimension: DimensionInfo = DimensionInfo()) {
  def merge(o: AggInfo): AggInfo = {
    this.cuboid.counter.merge(o.cuboid.counter)
    this
  }
}

/**
 * @param counter HLLCounter will could get est row count for specific cuboid
 */
case class CuboidInfo(counter: HLLCounter = new HLLCounter())

/**
 * @param data Maybe some sample data
 */
case class SampleInfo(data: Array[String] = new Array(3))

/**
 * @param range Maybe to save min/max of a specific dimension
 */
case class DimensionInfo(range: mutable.Map[String, String] = mutable.Map[String, String]())
