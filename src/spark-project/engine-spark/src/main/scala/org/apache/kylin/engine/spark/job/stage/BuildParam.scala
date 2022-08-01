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

package org.apache.kylin.engine.spark.job.stage

import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase.Statistics
import org.apache.kylin.engine.spark.job.stage.build.partition.PartitionFlatTableAndDictBase
import org.apache.kylin.engine.spark.model.{PartitionFlatTableDesc, SegmentFlatTableDesc}
import org.apache.kylin.metadata.cube.cuboid.{AdaptiveSpanningTree, PartitionSpanningTree}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.{immutable, mutable}

class BuildParam {
  private var spanningTree: AdaptiveSpanningTree = _
  private var flatTableDesc: SegmentFlatTableDesc = _
  private var factTableDS: Dataset[Row] = _
  private var fastFactTableDS: Dataset[Row] = _
  private var dict: Dataset[Row] = _
  private var flatTable: Dataset[Row] = _
  private var flatTablePart: Dataset[Row] = _
  private var buildFlatTable: FlatTableAndDictBase = _

  private var flatTableStatistics: Statistics = _

  private var tableDesc: PartitionFlatTableDesc = _
  private var partitionFlatTable: PartitionFlatTableAndDictBase = _
  private var partitionSpanningTree: PartitionSpanningTree = _
  private var cachedPartitionFlatTableDS: Map[java.lang.Long, Dataset[Row]] =
    immutable.Map.newBuilder[java.lang.Long, Dataset[Row]].result()
  private var cachedPartitionFlatTableStats: Map[java.lang.Long, Statistics] =
    immutable.Map.newBuilder[java.lang.Long, Statistics].result()

  private var skipGenerateFlatTable: Boolean = _
  private var skipMaterializedFactTableView: Boolean = _

  // thread unsafe
  private var cachedLayoutSanity: Option[Map[Long, Long]] = None
  // thread unsafe
  private var cachedLayoutDS = mutable.HashMap[Long, Dataset[Row]]()
  // thread unsafe
  private var cachedIndexInferior: Option[Map[Long, InferiorGroup]] = None

  def isSkipMaterializedFactTableView: Boolean = skipMaterializedFactTableView

  def setSkipMaterializedFactTableView(skipMaterializedFactTableView: Boolean): Unit = {
    this.skipMaterializedFactTableView = skipMaterializedFactTableView
  }

  def isSkipGenerateFlatTable: Boolean = skipGenerateFlatTable

  def setSkipGenerateFlatTable(skipGenerateFlatTable: Boolean): Unit = {
    this.skipGenerateFlatTable = skipGenerateFlatTable
  }

  def getCachedPartitionFlatTableStats: Map[java.lang.Long, Statistics] = cachedPartitionFlatTableStats

  def setCachedPartitionFlatTableStats(cachedPartitionFlatTableStats: Map[java.lang.Long, Statistics]): Unit = {
    this.cachedPartitionFlatTableStats = cachedPartitionFlatTableStats
  }

  def getCachedPartitionFlatTableDS: Map[java.lang.Long, Dataset[Row]] = cachedPartitionFlatTableDS

  def setCachedPartitionFlatTableDS(cachedPartitionFlatTableDS: Map[java.lang.Long, Dataset[Row]]): Unit = {
    this.cachedPartitionFlatTableDS = cachedPartitionFlatTableDS
  }

  def getPartitionSpanningTree: PartitionSpanningTree = partitionSpanningTree

  def setPartitionSpanningTree(partitionSpanningTree: PartitionSpanningTree): Unit = {
    this.partitionSpanningTree = partitionSpanningTree
  }

  def getPartitionFlatTable: PartitionFlatTableAndDictBase = partitionFlatTable

  def setPartitionFlatTable(partitionFlatTable: PartitionFlatTableAndDictBase): Unit = {
    this.partitionFlatTable = partitionFlatTable
  }

  def getTableDesc: PartitionFlatTableDesc = tableDesc

  def setTableDesc(tableDesc: PartitionFlatTableDesc): Unit = {
    this.tableDesc = tableDesc
  }

  def getFlatTableStatistics: Statistics = flatTableStatistics

  def setFlatTableStatistics(flatTableStatistics: Statistics): Unit = {
    this.flatTableStatistics = flatTableStatistics
  }

  def getBuildFlatTable: FlatTableAndDictBase = buildFlatTable

  def setBuildFlatTable(buildFlatTable: FlatTableAndDictBase): Unit = {
    this.buildFlatTable = buildFlatTable
  }

  def getFlatTable: Dataset[Row] = flatTable

  def setFlatTable(flatTable: Dataset[Row]): Unit = {
    this.flatTable = flatTable
  }

  def getFlatTablePart: Dataset[Row] = flatTablePart

  def setFlatTablePart(flatTablePart: Dataset[Row]): Unit = {
    this.flatTablePart = flatTablePart
  }

  def getDict: Dataset[Row] = dict

  def setDict(dict: Dataset[Row]): Unit = {
    this.dict = dict
  }

  def getFastFactTableDS: Dataset[Row] = fastFactTableDS

  def setFastFactTableDS(fastFactTableDS: Dataset[Row]): Unit = {
    this.fastFactTableDS = fastFactTableDS
  }

  def getFactTableDS: Dataset[Row] = factTableDS

  def setFactTableDS(factTableDS: Dataset[Row]): Unit = {
    this.factTableDS = factTableDS
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

  def getCachedLayoutDS: mutable.HashMap[Long, Dataset[Row]] = cachedLayoutDS

  def setCachedLayoutDS(cachedLayoutDS: mutable.HashMap[Long, Dataset[Row]]): Unit = {
    this.cachedLayoutDS = cachedLayoutDS
  }

  def getCachedIndexInferior: Option[Map[Long, InferiorGroup]] = cachedIndexInferior

  def setCachedIndexInferior(cachedIndexInferior: Option[Map[Long, InferiorGroup]]): Unit = {
    this.cachedIndexInferior = cachedIndexInferior
  }
}
