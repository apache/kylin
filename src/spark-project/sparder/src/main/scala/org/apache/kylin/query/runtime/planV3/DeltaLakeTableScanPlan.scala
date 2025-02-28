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
package org.apache.kylin.query.runtime.planV3

import org.apache.kylin.common.QueryContext
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate
import org.apache.kylin.metadata.cube.gridtable.NLayoutToGridTableMapping
import org.apache.kylin.metadata.cube.model.NDataflow
import org.apache.kylin.query.implicits.sessionToQueryContext
import org.apache.kylin.query.relnode.{OlapContext, OlapRel}
import org.apache.kylin.query.runtime.FilePruningMode.PruningMode
import org.apache.kylin.query.runtime.plan.TableScanPlan.{bucketEnabled, buildSchema}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.{SparderEnv, SparkOperation, SparkSession}

import scala.collection.JavaConverters._

object DeltaLakeTableScanPlan extends LogEx {

  def createOlapTable(rel: OlapRel, mode: PruningMode): LogicalPlan = logTime("table scan", debug = true) {
    val session: SparkSession = SparderEnv.getSparkSession
    val olapContext = rel.getContext
    val context = olapContext.getStorageContext
    val realizations = olapContext.getRealization.getRealizations.asScala.toList
    val plans = realizations.map(_.asInstanceOf[NDataflow])
      .map(dataflow => tableScan(rel, dataflow, olapContext, session, context.getCandidate, mode))

    // The reason why we use Project to package it here is because the output method of Union needs to be analyzed,
    // so it needs to be packaged by Project so that subsequent operators can easily obtain the output information of nodes.
    if (plans.size == 1) {
      plans.head
    } else {
      Project(plans.head.output, Union(plans))
    }
  }

  def tableScan(rel: OlapRel, dataflow: NDataflow, olapContext: OlapContext,
                session: SparkSession, candidate: NLayoutCandidate, pruningMode: PruningMode): LogicalPlan = {
    olapContext.resetSQLDigest()
    val cuboidLayout = candidate.getLayoutEntity
    if (cuboidLayout.getIndex != null && cuboidLayout.getIndex.isTableIndex) {
      QueryContext.current().getQueryTagInfo.setTableIndex(true)
    }
    val tableName = olapContext.getFirstTableScan.getBackupAlias
    val mapping = new NLayoutToGridTableMapping(cuboidLayout)
    val columnNames = SchemaProcessor.buildGTSchema(cuboidLayout, mapping, tableName)
    /////////////////////////////////////////////
    var plan = session.kylin
      .isFastBitmapEnabled(olapContext.isExactlyFastBitmap)
      .bucketingEnabled(bucketEnabled(olapContext, cuboidLayout))
      .filePruningMode(pruningMode)
      .cuboidTable(dataflow, cuboidLayout, null)

    plan = SparkOperation.projectAsAlias(columnNames, plan)
    val (schema, newPlan) = buildSchema(plan, tableName, cuboidLayout, rel, olapContext, dataflow)
    SparkOperation.project(schema, newPlan)
  }

}
