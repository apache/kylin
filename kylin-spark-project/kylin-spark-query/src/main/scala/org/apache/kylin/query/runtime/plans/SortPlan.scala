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
package org.apache.kylin.query.runtime.plans

import org.apache.calcite.DataContext
import org.apache.kylin.query.relnode.OLAPSortRel
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.spark.sql.DataFrame
import org.apache.spark.utils.LogEx
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.KylinFunctions._

object SortPlan extends LogEx {
  def sort(
    inputs: java.util.List[DataFrame],
    rel: OLAPSortRel,
    dataContext: DataContext): DataFrame = logTime("sort", info = true) {

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
