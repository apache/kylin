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

package org.apache.kylin.query.runtime

import java.util

import org.apache.kylin.shaded.com.google.common.collect.Lists
import org.apache.calcite.DataContext
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.kylin.query.relnode._
import org.apache.kylin.query.runtime.plans._
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
    if (!node.isInstanceOf[OLAPJoinRel]) {
      node.childrenAccept(this)
    } else if (node.asInstanceOf[OLAPJoinRel].isRuntimeJoin) {
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
            JoinPlan.join(Lists.newArrayList(left, right), rel)
          }
        }
      case rel: OLAPUnionRel =>
        val size = unionStack.pop()
        val java = Range(0, stack.size() - size).map(a => stack.pop()).asJava
        logTime("union") {
          UnionPlan.union(Lists.newArrayList(java), rel, dataContext)
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
