/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.{AliasHelper, IntegerLiteral, PredicateHelper, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimitAndOffset, LocalLimit, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan

object PostV2ScanRelationPushDown extends Rule[LogicalPlan] with PredicateHelper with AliasHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    val pushdownRules = Seq[LogicalPlan => LogicalPlan] (
      pushDownLimitAndOffsets)

    pushdownRules.foldLeft(plan) { (newPlan, pushDownRule) =>
      pushDownRule(newPlan)
    }
  }

  private def pushDownLimitAndOffset(plan: LogicalPlan, limit: Int): LogicalPlan = plan match {
    case relation @ DataSourceV2ScanRelation(_, wrapper: V1ScanWrapper, _) =>
      wrapper.v1Scan match {
        case jdbcScan: JDBCScan =>
          val newJDBCScan = jdbcScan.copy(pushedLimit = limit)
          val newPushedDownOperators = wrapper.pushedDownOperators.copy(limit = Some(limit))
          relation.copy(scan = wrapper.copy(v1Scan = newJDBCScan, pushedDownOperators = newPushedDownOperators))
        case _ => relation
      }
    case s @ Sort(order, _, operation @ ScanOperation(project, filter, relation: DataSourceV2ScanRelation))
      if filter.isEmpty && CollapseProject.canCollapseExpressions(
        order, project, alwaysInline = true) =>
      val aliasMap = getAliasMap(project)
      val newOrder = order.map(replaceAlias(_, aliasMap)).asInstanceOf[Seq[SortOrder]]
      val orders = DataSourceStrategy.translateSortOrders(newOrder)
      if (orders.length == order.length && relation.scan.isInstanceOf[V1ScanWrapper]) {
        val wrapper = relation.scan.asInstanceOf[V1ScanWrapper]
        wrapper.v1Scan match {
          case jdbcScan: JDBCScan if jdbcScan.relation.jdbcOptions.pushDownLimit =>
            val newJDBCScan = jdbcScan.copy(pushedLimit = limit, sortOrders = orders.toArray)
            val newPushedDownOperators = wrapper.pushedDownOperators.copy(limit = Some(limit), sortValues = orders)
            val newRelation =
              relation.copy(scan = wrapper.copy(v1Scan = newJDBCScan, pushedDownOperators = newPushedDownOperators))
            val newOperation = operation.withNewChildren(Seq(newRelation))
            if (jdbcScan.relation.jdbcOptions.numPartitions.map(_ == 1).getOrElse(true)) {
              newOperation
            } else {
              s.withNewChildren(Seq(newOperation))
            }
          case _ => s
        }
      } else {
        s
      }
    case p: Project =>
      val newChild = pushDownLimitAndOffset(p.child, limit)
      p.withNewChildren(Seq(newChild))
    case other => other
  }

  def pushDownLimitAndOffsets(plan: LogicalPlan): LogicalPlan = plan.transform {
    case globalLimitAndOffset @ GlobalLimitAndOffset(_, _, LocalLimit(IntegerLiteral(limitValue), child)) =>
      // limitValue is the sum of origin limit value and offset value.
      val newChild = pushDownLimitAndOffset(child, limitValue)
      val newLocalLimit = globalLimitAndOffset.child.asInstanceOf[LocalLimit].withNewChildren(Seq(newChild))
      globalLimitAndOffset.withNewChildren(Seq(newLocalLimit))
  }
}
