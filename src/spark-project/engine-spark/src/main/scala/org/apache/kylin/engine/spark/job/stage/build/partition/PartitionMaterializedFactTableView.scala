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

package org.apache.kylin.engine.spark.job.stage.build.partition

import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.model.PartitionFlatTableDesc
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser
import org.apache.kylin.metadata.cube.cuboid.PartitionSpanningTree
import org.apache.kylin.metadata.cube.cuboid.PartitionSpanningTree.PartitionTreeBuilder
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.spark.sql.{Dataset, Row}

class PartitionMaterializedFactTableView(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends PartitionFlatTableAndDictBase(jobContext, dataSegment, buildParam) {


  override def execute(): Unit = {
    logInfo(s"Build SEGMENT $segmentId")
    val spanTree = new PartitionSpanningTree(config, //
      new PartitionTreeBuilder(dataSegment, readOnlyLayouts, jobId, partitions))
    buildParam.setPartitionSpanningTree(spanTree)

    val tableDesc = if (jobContext.isPartialBuild) {
      val parser = new IndexDependencyParser(dataModel)
      val relatedTableAlias =
        parser.getRelatedTablesAlias(jobContext.getReadOnlyLayouts)
      new PartitionFlatTableDesc(config, dataSegment, spanTree, relatedTableAlias, jobId, partitions)
    } else {
      new PartitionFlatTableDesc(config, dataSegment, spanTree, jobId, partitions)
    }
    buildParam.setTableDesc(tableDesc)

    val factTableDS: Dataset[Row] = newFactTableDS()
    buildParam.setFactTableDS(factTableDS)

    val fastFactTableDS: Dataset[Row] = newFastFactTableDS()
    buildParam.setFastFactTableDS(fastFactTableDS)

    if (buildParam.isSkipMaterializedFactTableView) {
      onStageSkipped()
    }
  }
}
