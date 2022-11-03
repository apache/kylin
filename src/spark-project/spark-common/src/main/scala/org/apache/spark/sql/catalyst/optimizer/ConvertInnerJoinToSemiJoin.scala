/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{JOIN, PROJECT}

class ConvertInnerJoinToSemiJoin extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAllPatterns(PROJECT, JOIN)) {
    case p @ Project(_, j @ ExtractEquiJoinKeys(_, _, _, nonEquiCond, _, agg: Aggregate, _))
        if innerEqualJoin(j, nonEquiCond) & canTransform(agg, j, p) =>
      // eliminate the agg, replace it with a project with all its expressions
      p.copy(child = j.copy(joinType = LeftSemi, right = Project(agg.aggregateExpressions, agg.child)))
    case p @ Project(_, j @ ExtractEquiJoinKeys(_, _, _, nonEquiCond, agg: Aggregate, _, _))
      if innerEqualJoin(j, nonEquiCond) & canTransform(agg, j, p) =>
      // eliminate the agg, replace it with a project with all its expressions
      // it will swap the join order from inner join(left, right) to semi join(right, left)
      p.copy(child = j.copy(joinType = LeftSemi, left = j.right, right = Project(agg.aggregateExpressions, agg.child)))
  }

  def innerEqualJoin(join: Join, nonEquiCond: Option[Expression]): Boolean = {
    join.joinType == Inner && join.condition.nonEmpty && nonEquiCond.isEmpty
  }

  def canTransform(agg: Aggregate, join: Join, project: Project): Boolean = {
    // 1. grouping only - the agg output and only output all the grouping cols
    // 2. all grouping cols should be in join condition
    // 3. all grouping cols should not appear in the project above
    exactGroupingOnly(agg) &&
      agg.aggregateExpressions.forall(join.condition.get.references.contains(_)) &&
      agg.aggregateExpressions.forall(!project.references.contains(_))
  }

  // test if the aggregate exprs and grouping exprs are exactly the same
  def exactGroupingOnly(agg: Aggregate): Boolean = {
    if (agg.aggregateExpressions.size != agg.groupingExpressions.size) {
      return false
    }

    val foundInGroupings = agg.aggregateExpressions.map {
      case Alias(child, _) =>
        agg.groupingExpressions.indexWhere(_.semanticEquals(child))
      case other =>
        agg.groupingExpressions.indexWhere(_.semanticEquals(other))
    }.toSet
    // all agg expr should be found in grouping expr
    // and the number should match
    !foundInGroupings.contains(-1) && foundInGroupings.size == agg.groupingExpressions.size
  }
}
