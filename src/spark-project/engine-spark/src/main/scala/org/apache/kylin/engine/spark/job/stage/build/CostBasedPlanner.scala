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

package org.apache.kylin.engine.spark.job.stage.build

import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import org.apache.kylin.metadata.cube.model.NDataSegment

class CostBasedPlanner(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends FlatTableAndDictBase(jobContext, dataSegment, buildParam) {
  override def execute(): Unit = {
    val (cost, sourceCount) = generateCostTable()
    getRecommendedLayoutAndUpdateMetadata(cost, sourceCount)

    val result = jobContext.updateIndexPlanIfNeed()
    if (result) {
      // update span tree and table desc with the new build job layouts
      val spanTree = new AdaptiveSpanningTree(config, new AdaptiveTreeBuilder(dataSegment, readOnlyLayouts))
      buildParam.setSpanningTree(spanTree)

      // update table desc with new span tree
      val flatTableDesc: SegmentFlatTableDesc = if (jobContext.isPartialBuild) {
        val parser = new IndexDependencyParser(dataModel)
        val relatedTableAlias =
          parser.getRelatedTablesAlias(readOnlyLayouts)
        new SegmentFlatTableDesc(config, dataSegment, spanningTree, relatedTableAlias)
      } else {
        new SegmentFlatTableDesc(config, dataSegment, spanningTree)
      }
      buildParam.setFlatTableDesc(flatTableDesc)
    }
  }

  override def getStageName: String = "CostBasedPlanner"
}
