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

import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.calcite.DataContext
import org.apache.kylin.query.relnode.KapSortRel
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.KapFunctions._

import scala.collection.JavaConverters._


object SortPlan extends LogEx {
  def sort(inputs: java.util.List[DataFrame],
           rel: KapSortRel,
           dataContext: DataContext): DataFrame = logTime("sort", debug = true) {

    val dataFrame = inputs.get(0)

    val columns = rel.getChildExps.asScala
      .map(rex => {
        val visitor = new SparderRexVisitor(dataFrame,
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
            pair._1.asc_nulls_last
          case (org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
                org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST) =>
            pair._1.asc_nulls_last
          case (org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
                org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST) =>
            pair._1.asc_nulls_first
          case (
              org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
              org.apache.calcite.rel.RelFieldCollation.NullDirection.UNSPECIFIED) =>
            pair._1.desc_nulls_first
          case (org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
                org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST) =>
            pair._1.desc_nulls_last
          case (org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
                org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST) =>
            pair._1.desc_nulls_first
          case _ => throw new IllegalArgumentException
        }
      })

    inputs.get(0).sort(columns: _*)
  }
}
