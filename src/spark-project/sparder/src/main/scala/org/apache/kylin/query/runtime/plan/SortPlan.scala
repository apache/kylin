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
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.query.relnode.OlapSortRel
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.spark.sql.KapFunctions.k_lit
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, NullsFirst, NullsLast, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}

import scala.collection.JavaConverters._

object SortPlan extends LogEx {
  def sort(plan: LogicalPlan,
           rel: OlapSortRel,
           dataContext: DataContext): LogicalPlan = logTime("sort", debug = true) {

    val columns = rel.getSortExps.asScala
      .map(rex => {
        val visitor = new SparderRexVisitor(plan.output.map(_.name),
          rel.getInput.getRowType,
          dataContext)
        rex.accept(visitor)
      })
      .map(c => k_lit(c))
      .zipWithIndex
      .map(pair => {
        val collation = rel.collation.getFieldCollations.get(pair._2)

        /** From Calcite: org.apache.calcite.rel.RelFieldCollation.Direction#defaultNullDirection
         * Returns the null direction if not specified. Consistent with Oracle,
         * NULLS are sorted as if they were positive infinity. */
        (collation.direction, collation.nullDirection) match {
          case (
            org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
            org.apache.calcite.rel.RelFieldCollation.NullDirection.UNSPECIFIED) =>
            SortOrder(pair._1.expr, Ascending, NullsLast, Seq.empty)
          case (org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
          org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST) =>
            SortOrder(pair._1.expr, Ascending, NullsLast, Seq.empty)
          case (org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
          org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST) =>
            SortOrder(pair._1.expr, Ascending, NullsFirst, Seq.empty)
          case (
            org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
            org.apache.calcite.rel.RelFieldCollation.NullDirection.UNSPECIFIED) =>
            SortOrder(pair._1.expr, Descending, NullsFirst, Seq.empty)
          case (org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
          org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST) =>
            SortOrder(pair._1.expr, Descending, NullsLast, Seq.empty)
          case (org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
          org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST) =>
            SortOrder(pair._1.expr, Descending, NullsFirst, Seq.empty)
          case _ => throw new IllegalArgumentException
        }
      })

    Sort(columns, true, plan)
  }
}
