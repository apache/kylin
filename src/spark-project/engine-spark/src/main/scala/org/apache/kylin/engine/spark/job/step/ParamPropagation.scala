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

package org.apache.kylin.engine.spark.job.step

import org.apache.kylin.engine.spark.job.step.build.FlatTableStage
import org.apache.kylin.engine.spark.job.step.build.FlatTableStage.Statistics
import org.apache.kylin.engine.spark.job.step.build.partition.PartitionFlatTableStage
import org.apache.kylin.engine.spark.model.{PartitionFlatTableDesc, SegmentFlatTableDesc}
import org.apache.kylin.metadata.cube.cuboid.{AdaptiveSpanningTree, PartitionSpanningTree}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.{immutable, mutable}

/**
 * Propagate an stage's parameters to next one.
 */
class ParamPropagation {
  private var spanningTree: AdaptiveSpanningTree = _
  private var flatTableDesc: SegmentFlatTableDesc = _
  private var factTable: Dataset[Row] = _
  private var partFactTable: Dataset[Row] = _
  private var dict: Dataset[Row] = _
  private var flatTable: Dataset[Row] = _
  private var lightFlatTable: Dataset[Row] = _
  private var flatTableStage: FlatTableStage = _

  private var flatTableStats: Statistics = _

  private var tableDesc: PartitionFlatTableDesc = _
  private var partitionFlatTable: PartitionFlatTableStage = _
  private var partitionSpanningTree: PartitionSpanningTree = _
  // Partition flat table.
  private var cachedPartitions: Map[Long, Dataset[Row]] =
    immutable.Map.newBuilder[Long, Dataset[Row]].result()
  // Partition flat table statistics.
  private var cachedPartitionStats: Map[Long, Statistics] =
    immutable.Map.newBuilder[Long, Statistics].result()

  private var skipPersistFactView: Boolean = _
  private var skipPersistFlatTable: Boolean = _

  // thread unsafe
  private var cachedLayoutSanity: Option[Map[Long, Long]] = None
  // thread unsafe
  private var cachedLayouts = mutable.HashMap[Long, Dataset[Row]]()
  // thread unsafe
  private var cachedIndexInferior: Option[Map[Long, InferiorGroup]] = None

  // Thread unsafe
  // [layout, [partition, dataset]]
  private var cachedLayoutPartitions = mutable.HashMap[Long, mutable.HashMap[Long, Dataset[Row]]]()

  // Thread unsafe
  // [layout, [partition, sanity]]
  private var cachedLayoutPartitionSanity: Option[mutable.HashMap[Long, mutable.HashMap[Long, Long]]] = None

  def isSkipPersistFactView: Boolean = skipPersistFactView

  def setSkipPersistFactView(skipPersistFactView: Boolean): Unit = {
    this.skipPersistFactView = skipPersistFactView
  }

  def isSkipPersistFlatTable: Boolean = skipPersistFlatTable

  def setSkipPersistFlatTable(skipPersistFlatTable: Boolean): Unit = {
    this.skipPersistFlatTable = skipPersistFlatTable
  }

  def getCachedPartitionStats: Map[Long, Statistics] = cachedPartitionStats

  def setCachedPartitionStats(cachedPartitionStats: Map[Long, Statistics]): Unit = {
    this.cachedPartitionStats = cachedPartitionStats
  }

  def getCachedPartitions: Map[Long, Dataset[Row]] = cachedPartitions

  def setCachedPartitions(cachedPartitions: Map[Long, Dataset[Row]]): Unit = {
    this.cachedPartitions = cachedPartitions
  }

  def getPartitionSpanningTree: PartitionSpanningTree = partitionSpanningTree

  def setPartitionSpanningTree(partitionSpanningTree: PartitionSpanningTree): Unit = {
    this.partitionSpanningTree = partitionSpanningTree
  }

  def getPartitionFlatTable: PartitionFlatTableStage = partitionFlatTable

  def setPartitionFlatTable(partitionFlatTable: PartitionFlatTableStage): Unit = {
    this.partitionFlatTable = partitionFlatTable
  }

  def getTableDesc: PartitionFlatTableDesc = tableDesc

  def setTableDesc(tableDesc: PartitionFlatTableDesc): Unit = {
    this.tableDesc = tableDesc
  }

  def getFlatTableStats: Statistics = flatTableStats

  def setFlatTableStats(flatTableStatistics: Statistics): Unit = {
    this.flatTableStats = flatTableStatistics
  }

  def getFlatTableStage: FlatTableStage = flatTableStage

  def setFlatTableStage(flatTableStage: FlatTableStage): Unit = {
    this.flatTableStage = flatTableStage
  }

  def getFlatTable: Dataset[Row] = flatTable

  def setFlatTable(flatTable: Dataset[Row]): Unit = {
    this.flatTable = flatTable
  }

  def getLightFlatTable: Dataset[Row] = lightFlatTable

  def setLightFlatTable(lightFlatTable: Dataset[Row]): Unit = {
    this.lightFlatTable = lightFlatTable
  }

  def getDict: Dataset[Row] = dict

  def setDict(dict: Dataset[Row]): Unit = {
    this.dict = dict
  }

  def getPartFactTable: Dataset[Row] = partFactTable

  def setPartFactTable(partFactTable: Dataset[Row]): Unit = {
    this.partFactTable = partFactTable
  }

  def getFactTable: Dataset[Row] = factTable

  def setFactTable(factTable: Dataset[Row]): Unit = {
    this.factTable = factTable
  }

  def getFlatTableDesc: SegmentFlatTableDesc = flatTableDesc

  def setFlatTableDesc(flatTableDesc: SegmentFlatTableDesc): Unit = {
    this.flatTableDesc = flatTableDesc
  }

  def getSpanningTree: AdaptiveSpanningTree = spanningTree

  def setSpanningTree(spanningTree: AdaptiveSpanningTree): Unit = {
    this.spanningTree = spanningTree
  }

  def getCachedLayoutSanity: Option[Map[Long, Long]] = cachedLayoutSanity

  def setCachedLayoutSanity(cachedLayoutSanity: Option[Map[Long, Long]]): Unit = {
    this.cachedLayoutSanity = cachedLayoutSanity
  }

  def getCachedLayouts: mutable.HashMap[Long, Dataset[Row]] = cachedLayouts

  def setCachedLayouts(cachedLayouts: mutable.HashMap[Long, Dataset[Row]]): Unit = {
    this.cachedLayouts = cachedLayouts
  }

  def getCachedIndexInferior: Option[Map[Long, InferiorGroup]] = cachedIndexInferior

  def setCachedIndexInferior(cachedIndexInferior: Option[Map[Long, InferiorGroup]]): Unit = {
    this.cachedIndexInferior = cachedIndexInferior
  }

  def getCachedLayoutPartitions: mutable.HashMap[Long, mutable.HashMap[Long, Dataset[Row]]] = cachedLayoutPartitions

  def setCachedLayoutPartitions(cachedLayoutPartitions: mutable.HashMap[Long, mutable.HashMap[Long, Dataset[Row]]]): Unit = {
    this.cachedLayoutPartitions = cachedLayoutPartitions
  }

  def getCachedLayoutPartitionSanity: Option[mutable.HashMap[Long, mutable.HashMap[Long, Long]]] = cachedLayoutPartitionSanity

  def setCachedLayoutPartitionSanity(cachedPartitionSanity: Option[mutable.HashMap[Long, mutable.HashMap[Long, Long]]]): Unit = {
    this.cachedLayoutPartitionSanity = cachedPartitionSanity
  }
}
