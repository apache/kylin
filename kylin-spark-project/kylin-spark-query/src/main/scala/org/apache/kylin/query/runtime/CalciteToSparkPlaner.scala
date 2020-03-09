/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */

package org.apache.kylin.query.runtime

import java.util

import com.google.common.collect.Lists
import org.apache.calcite.DataContext
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.kylin.query.relnode.{OLAPAggregateRel, OLAPFilterRel, OLAPJoinRel, OLAPLimitRel, OLAPProjectRel, OLAPSortRel, OLAPTableScan, OLAPUnionRel, OLAPValuesRel, OLAPWindowRel}
import org.apache.kylin.query.runtime.plans.{AggregatePlan, FilterPlan, LimitPlan, ProjectPlan, SortPlan, TableScanPlan, ValuesPlan, WindowPlan}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class CalciteToSparkPlaner(dataContext: DataContext) extends RelVisitor with Logging {
  private val stack = new util.Stack[DataFrame]()
  private val unionStack = new util.Stack[Int]()

  override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
    if (node.isInstanceOf[OLAPUnionRel]) {
      unionStack.push(stack.size())
    }
    // skip non runtime joins children
    // cases to skip children visit
    // 1. current node is a OLAPJoinRel and is not a runtime join
    if (!node.isInstanceOf[OLAPJoinRel] && node.asInstanceOf[OLAPJoinRel].isRuntimeJoin) {
      node.childrenAccept(this)
    }
    stack.push(node match {
      case rel: OLAPTableScan =>
        rel.genExecFunc() match {
          case "executeLookupTableQuery" =>
            logTime("executeLookupTableQuery") {
              TableScanPlan.createLookupTable(rel, dataContext)
            }
          case "executeOLAPQuery" =>
            logTime("executeOLAPQuery") {
              TableScanPlan.createOLAPTable(rel, dataContext)
            }
          case "executeSimpleAggregationQuery" =>
            logTime("executeSimpleAggregationQuery") {
              TableScanPlan.createSingleRow(rel, dataContext)
            }
        }
      case rel: OLAPFilterRel =>
        logTime("filter") {
          FilterPlan.filter(Lists.newArrayList(stack.pop()), rel, dataContext)
        }
      case rel: OLAPProjectRel =>
        logTime("project") {
          ProjectPlan.select(Lists.newArrayList(stack.pop()), rel, dataContext)
        }
      case rel: OLAPLimitRel =>
        logTime("limit") {
          LimitPlan.limit(Lists.newArrayList(stack.pop()), rel, dataContext)
        }
      case rel: OLAPSortRel =>
        logTime("sort") {
          SortPlan.sort(Lists.newArrayList(stack.pop()), rel, dataContext)
        }
      case rel: OLAPWindowRel =>
        logTime("window") {
          WindowPlan.window(Lists.newArrayList(stack.pop()), rel, dataContext)
        }
      case rel: OLAPAggregateRel =>
        logTime("agg") {
          AggregatePlan.agg(Lists.newArrayList(stack.pop()), rel, dataContext)
        }
      case rel: OLAPJoinRel =>
        if (!rel.isRuntimeJoin) {
          logTime("join with table scan") {
            TableScanPlan.createOLAPTable(rel, dataContext)
          }
        } else {
          val right = stack.pop()
          val left = stack.pop()
          logTime("join") {
            plans.JoinPlan.join(Lists.newArrayList(left, right), rel)
          }
        }
      case rel: OLAPUnionRel =>
        val size = unionStack.pop()
        val java = Range(0, stack.size() - size).map(a => stack.pop()).asJava
        logTime("union") {
          plans.UnionPlan.union(Lists.newArrayList(java), rel, dataContext)
        }
      case rel: OLAPValuesRel =>
        logTime("values") {
          ValuesPlan.values(rel)
        }
    })
  }

  def getResult(): DataFrame = {
    stack.pop()
  }

  def logTime[U](plan: String)(body: => U): U = {
    val start = System.currentTimeMillis()
    val result = body
    logTrace(s"Run $plan take ${System.currentTimeMillis() - start}")
    result
  }
}
