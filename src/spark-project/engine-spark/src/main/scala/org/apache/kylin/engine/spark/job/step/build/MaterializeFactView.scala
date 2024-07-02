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

package org.apache.kylin.engine.spark.job.step.build

import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.step.ParamPropagation
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataSegment}
import org.apache.spark.sql.{Dataset, Row}

class MaterializeFactView(jobContext: SegmentJob, dataSegment: NDataSegment, params: ParamPropagation)
  extends FlatTableStage(jobContext, dataSegment, params) {

  override def execute(): Unit = {
    logInfo(s"Build SEGMENT $segmentId")
    persistFactView()
    if (params.isSkipPersistFactView) {
      onStageSkipped()
    }
  }

  override def getStageName: String = "MaterializeFactView"

  // just moved from 4.5.x, needs refactor...
  def computeLayoutFromSourceAllInOne(layoutEntity: LayoutEntity): Dataset[Row] = {
    val spanTree = new AdaptiveSpanningTree(config,
      new AdaptiveTreeBuilder(dataSegment, ImmutableSet.of(layoutEntity)))
    params.setSpanningTree(spanTree)

    val flatTableDesc = new SegmentFlatTableDesc(config, dataSegment, spanTree)
    params.setFlatTableDesc(flatTableDesc)

    val partFactTable = createPartFactTable()
    params.setPartFactTable(partFactTable)

    val dict = buildDictIfNeed()
    params.setDict(dict)

    val flatTable = dict // YES, the 'dict' is actually 'flatTable'
    params.setFlatTable(flatTable)

    // flat table ==> layout DS
    val parentDS = flatTable.select(columnsFromFlatTable(layoutEntity.getIndex).map(org.apache.spark.sql.functions.col): _*)
    val layoutDS = wrapLayoutDS(layoutEntity, parentDS)

    layoutDS
  }
}
