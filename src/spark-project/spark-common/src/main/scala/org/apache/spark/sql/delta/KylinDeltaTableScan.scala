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
package org.apache.spark.sql.delta

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.PROJECT
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.LogicalRelation

object KylinDeltaTableScan {

  /**
   * The components of DeltaTableScanType are:
   * - the plan with removed projections. We remove projections as a plan differentiator
   * because it does not affect file listing results.
   * - filter expressions collected by `PhysicalOperation`
   * - the `TahoeLogFileIndex` of the matched DeltaTable`
   * - integer value of limit expression, if any
   * - matched `DeltaTable`
   */
  type DeltaTableScanType =
    (LogicalPlan, Seq[Expression], TahoeLogFileIndex, Option[Int], LogicalRelation)

  /**
   * This is an extractor method (basically, the opposite of a constructor) which takes in an
   * object `plan` and tries to give back the arguments as a [[DeltaTableScanType]].
   */
  def unapply(plan: LogicalPlan): Option[DeltaTableScanType] = {
    // Remove projections as a plan differentiator because it does not affect file listing
    // results. Plans with the same filters but different projections therefore will not have
    // duplicate delta indexes.
    def canonicalizePlanForDeltaFileListing(plan: LogicalPlan): LogicalPlan = {
      val planWithRemovedProjections = plan.transformWithPruning(_.containsPattern(PROJECT)) {
        case p: Project if p.projectList.forall(_.isInstanceOf[AttributeReference]) => p.child
      }
      planWithRemovedProjections
    }

    plan match {
      case PhysicalOperation(
      _,
      filters,
      delta@DeltaTable(fileIndex: TahoeLogFileIndex)) =>
        val allFilters = fileIndex.partitionFilters ++ filters
        Some((canonicalizePlanForDeltaFileListing(plan), allFilters, fileIndex, None, delta))

      case _ => None
    }
  }
}
