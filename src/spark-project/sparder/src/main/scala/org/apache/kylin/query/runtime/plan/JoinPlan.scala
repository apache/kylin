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
package org.apache.kylin.query.runtime.plan

import org.apache.calcite.DataContext
import org.apache.calcite.rex.RexCall
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.query.relnode.{OlapJoinRel, OlapNonEquiJoinRel}
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.kylin.query.util.OlapRelUtil
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.{Cross, JoinType}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, SparkOperation}

import java.util
import scala.collection.JavaConverters._

object JoinPlan extends LogEx {
  def nonEquiJoin(plans: Seq[LogicalPlan],
                  rel: OlapNonEquiJoinRel, dataContext: DataContext): LogicalPlan = {
    var lPlan = plans.head
    var rPlan = plans.apply(1)

    lPlan = SparkOperation.project(lPlan.output.map(c => col(c.name).alias("l_" + c.name)), lPlan)
    rPlan = SparkOperation.project(rPlan.output.map(c => col(c.name).alias("r_" + c.name)), rPlan)
    // slice lSchemaNames with rel.getLeftInputSizeBeforeRewrite
    // to strip off the fields added during rewrite
    // as those field will disturb the original index based join condition
    val visitor = new SparderRexVisitor(Seq(lPlan.output.map(c => c.name).slice(0, rel.getLeftInputSizeBeforeRewrite), rPlan.output.map(c => c.name)).flatten,
      null,
      dataContext)
    val pairs = new util.ArrayList[org.apache.kylin.common.util.Pair[Integer, Integer]]()
    val filterNuls = new util.ArrayList[java.lang.Boolean]()
    val actRemaining = OlapRelUtil.isNotDistinctFrom(rel.getInput(0), rel.getInput(1), rel.getCondition, pairs, filterNuls)
    if (filterNuls.contains(java.lang.Boolean.FALSE)) {
      var equalCond = makeEqualCond(col(visitor.inputFieldNames.apply(pairs.get(0).getFirst)),
        col(visitor.inputFieldNames.apply(pairs.get(0).getSecond)), !filterNuls.get(0))

      var i = 1
      while (i < filterNuls.size()) {
        equalCond = equalCond.and(makeEqualCond(col(visitor.inputFieldNames.apply(pairs.get(i).getFirst)),
          col(visitor.inputFieldNames.apply(pairs.get(i).getSecond)), !filterNuls.get(i)))
        i = i + 1
      }

      if (actRemaining != null && actRemaining.isInstanceOf[RexCall]) {
        equalCond = equalCond.and(actRemaining.accept(visitor).asInstanceOf[Column])
      }

      Join(lPlan, rPlan, joinType = JoinType(rel.getJoinType.lowerName), Some(equalCond.expr), JoinHint.NONE)
    } else {
      val conditionExprCol: Column = rel.getCondition.accept(visitor) match {
        case bool: Boolean =>
          // Calcite optimize condition in JoinRel
          new Column(Literal(bool, DataTypes.BooleanType))
        case expr =>
          expr.asInstanceOf[Column]
      }
      Join(lPlan, rPlan, joinType = JoinType(rel.getJoinType.lowerName), Some(conditionExprCol.expr), JoinHint.NONE)
    }
  }

  // scalastyle:off
  def join(plans: Seq[LogicalPlan],
           rel: OlapJoinRel): LogicalPlan = {

    var lPlan = plans.head
    var rPlan = plans.apply(1)

    lPlan = SparkOperation.project(lPlan.output.map(c => col(c.name).alias("l_" + c.name)), lPlan)
    rPlan = SparkOperation.project(rPlan.output.map(c => col(c.name).alias("r_" + c.name)), rPlan)

    var joinCol: Column = null

    //  todo   utils
    rel.getLeftKeys.asScala
      .zip(rel.getRightKeys.asScala)
      .foreach(tuple => {
        val col1 = col(lPlan.output.apply(tuple._1).name)
        val col2 = col(rPlan.output.apply(tuple._2).name)
        val equalCond = makeEqualCond(col1, col2, rel.isJoinCondEqualNullSafe)

        if (joinCol == null) {
          joinCol = equalCond
        } else {
          joinCol = joinCol.and(equalCond)
        }
      })
    if (joinCol == null) {
      Join(lPlan, rPlan, joinType = Cross, None, JoinHint.NONE)
    } else {
      Join(lPlan, rPlan, joinType = JoinType(rel.getJoinType.lowerName), Some(joinCol.expr), JoinHint.NONE)
    }
  }

  private def makeEqualCond(col1: Column, col2: Column, nullSafe: Boolean): Column = {
    if (nullSafe) {
      col1.eqNullSafe(col2)
    } else {
      col1.equalTo(col2)
    }
  }
}
