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

package org.apache.kylin.engine.spark.builder

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.step.ParamPropagation
import org.apache.kylin.engine.spark.job.step.build.FlatTableStage
import org.apache.kylin.engine.spark.job.step.build.FlatTableStage.createTable
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TableRef
import org.apache.spark.sql.{Dataset, Row}

class TestFlatTableStage(jobContext: SegmentJob, dataSegment: NDataSegment, params: ParamPropagation)
  extends FlatTableStage(jobContext, dataSegment, params) {
  def test(kylinConfig: KylinConfig, toBuildTree: AdaptiveSpanningTree): Unit = {
    params.setSpanningTree(toBuildTree)
    val flatTableDesc = new SegmentFlatTableDesc(kylinConfig, dataSegment, toBuildTree)
    params.setFlatTableDesc(flatTableDesc)
    val factTable: Dataset[Row] = createFactTable()
    params.setFactTable(factTable)
    val partFactTable: Dataset[Row] = createPartFactTable()
    params.setPartFactTable(partFactTable)
    val dict: Dataset[Row] = buildDictIfNeed()
    params.setDict(dict)
    val flatTable: Dataset[Row] = createFlatTable()
    params.setFlatTable(flatTable)
    val lightFlatTable: Dataset[Row] = createLightFlatTable()
    params.setLightFlatTable(lightFlatTable)
  }

  def testCreateTable(ref: TableRef): Dataset[Row] = {
    createTable(ref)(jobContext.getSparkSession)
  }
}
