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
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TableRef
import org.apache.spark.sql.{Dataset, Row}

class TestFlatTable(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends FlatTableAndDictBase(jobContext, dataSegment, buildParam) {
  def test(kylinConfig: KylinConfig, toBuildTree: AdaptiveSpanningTree): Unit = {
    buildParam.setSpanningTree(toBuildTree)
    val flatTableDesc = new SegmentFlatTableDesc(kylinConfig, dataSegment, toBuildTree)
    buildParam.setFlatTableDesc(flatTableDesc)
    val factTableDS: Dataset[Row] = newFactTableDS()
    buildParam.setFactTableDS(factTableDS)
    val fastFactTableDS: Dataset[Row] = newFastFactTableDS()
    buildParam.setFastFactTableDS(fastFactTableDS)
    val dict: Dataset[Row] = buildDictIfNeed()
    buildParam.setDict(dict)
    val flatTable: Dataset[Row] = generateFlatTable()
    buildParam.setFlatTable(flatTable)
    val flatTablePart: Dataset[Row] = generateFlatTablePart()
    buildParam.setFlatTablePart(flatTablePart)
  }

  def testNewTableDS(ref: TableRef): Dataset[Row] = {
    newTableDS(ref)
  }
}
