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

import java.util
import org.apache.calcite.DataContext
import org.apache.calcite.rex.RexCall
import org.apache.kylin.query.relnode.{KapJoinRel, KapNonEquiJoinRel}
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.kylin.query.util.KapRelUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._

object JoinPlan {
  def nonEquiJoin(inputs: java.util.List[DataFrame],
           rel: KapNonEquiJoinRel, dataContext: DataContext): DataFrame = {
    val lDataFrame = inputs.get(0)
    val rDataFrame = inputs.get(1)
    val lSchemaNames = lDataFrame.schema.fieldNames.map("l_" + _)
    val rSchemaNames = rDataFrame.schema.fieldNames.map("r_" + _)
    // val schema = statefulDF.indexSchema
    val newLDataFrame = inputs.get(0).toDF(lSchemaNames: _*)
    val newRDataFrame = inputs.get(1).toDF(rSchemaNames: _*)
    // slice lSchemaNames with rel.getLeftInputSizeBeforeRewrite
    // to strip off the fields added during rewrite
    // as those field will disturb the original index based join condition
    val visitor = new SparderRexVisitor(Array(lSchemaNames.slice(0, rel.getLeftInputSizeBeforeRewrite), rSchemaNames).flatten,
      null,
      dataContext)
    val pairs = new util.ArrayList[org.apache.kylin.common.util.Pair[Integer, Integer]]()
    val filterNuls = new util.ArrayList[java.lang.Boolean]()
    val actRemaining = KapRelUtil.isNotDistinctFrom(rel.getInput(0), rel.getInput(1), rel.getCondition, pairs, filterNuls)
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

      newLDataFrame.join(newRDataFrame, equalCond, rel.getJoinType.lowerName)
    } else {
      val conditionExprCol = rel.getCondition.accept(visitor).asInstanceOf[Column]
      newLDataFrame.join(newRDataFrame, conditionExprCol, rel.getJoinType.lowerName)
    }
  }

  def join(inputs: java.util.List[DataFrame],
           rel: KapJoinRel): DataFrame = {

    val lDataFrame = inputs.get(0)
    val rDataFrame = inputs.get(1)
    val lSchemaNames = lDataFrame.schema.fieldNames.map("l_" + _)
    val rSchemaNames = rDataFrame.schema.fieldNames.map("r_" + _)
    // val schema = statefulDF.indexSchema
    val newLDataFrame = inputs.get(0).toDF(lSchemaNames: _*)
    val newRDataFrame = inputs.get(1).toDF(rSchemaNames: _*)
    var joinCol: Column = null

    //  todo   utils
    rel.getLeftKeys.asScala
      .zip(rel.getRightKeys.asScala)
      .foreach(tuple => {
        val col1 = col(lSchemaNames.apply(tuple._1))
        val col2 = col(rSchemaNames.apply(tuple._2))
        val equalCond = makeEqualCond(col1, col2, rel.isJoinCondEqualNullSafe)

        if (joinCol == null) {
          joinCol = equalCond
        } else {
          joinCol = joinCol.and(equalCond)
        }
      })
    if (joinCol == null) {
      newLDataFrame.crossJoin(newRDataFrame)
    } else {
      newLDataFrame.join(newRDataFrame, joinCol, rel.getJoinType.lowerName)
    }
  }

  def makeEqualCond(col1: Column, col2: Column, nullSafe: Boolean): Column = {
    if (nullSafe) {
      col1.eqNullSafe(col2)
    } else {
      col1.equalTo(col2)
    }
  }
}
