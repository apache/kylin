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

package org.apache.kylin.engine.spark.builder.v3dict

import org.apache.spark.sql.SparkInternalAgent.{createColumn, getDataFrame, getLogicalPlan}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, DictEncodeV3, EqualTo, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{LongType, StringType}

class PreCountDistinctTransformer(spark: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case project@ Project(_, child) =>
      val relatedFields = scala.collection.mutable.Queue[CountDistExprInfo]()
      project.transformExpressions {
        case DictEncodeV3(child, dbName) =>
          val deAttr = AttributeReference("dict_encoded_" + child.prettyName, LongType,
            nullable = false)(NamedExpression.newExprId, Seq.empty[String])
          relatedFields += CountDistExprInfo(child, deAttr, dbName)
          createColumn(deAttr).expr
      }.withNewChildren {
        val dictionaries = relatedFields.map {
          case CountDistExprInfo(childExpr, encodedAttr, dbName) =>
            val windowSpec = Window.orderBy(createColumn(childExpr))
            val exprName = childExpr match {
              case ne: NamedExpression => ne.name
              case _ => childExpr.prettyName
            }
            logInfo(s"Count distinct expr name $exprName")
            val dictPlan = GlobalDictionaryPlaceHolder(exprName, getLogicalPlan(
              getDataFrame(spark, child).groupBy(createColumn(childExpr)).agg(
                createColumn(childExpr)).select(
                createColumn(childExpr).cast(StringType) as "dict_key",
                row_number().over(windowSpec).cast(LongType) as "dict_value")), dbName)
            val key = dictPlan.output.head
            val value = dictPlan.output(1)
            val valueAlias = Alias(value, encodedAttr.name)(encodedAttr.exprId)
            (Project(Seq(key, valueAlias), dictPlan), (childExpr, encodedAttr), dbName)
        }

        val result = dictionaries.foldLeft(child) {
          (joined, dict) =>
            val childExpr = dict._2._1
            val encodedAttr = dict._2._2
            val map = dict._1
            Project(joined.output :+ encodedAttr,
              Join(joined,
                map,
                LeftOuter,
                Some(EqualTo(childExpr, map.projectList.head)),
                JoinHint.NONE))
        }

        Seq(result)
      }
  }
}

case class CountDistExprInfo(childExpr: Expression, encodedAttr: AttributeReference, dbName: String)
