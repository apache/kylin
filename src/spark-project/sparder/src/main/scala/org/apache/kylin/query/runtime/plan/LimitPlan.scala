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
import org.apache.kylin.query.relnode.OlapLimitRel
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan, Offset}

object LimitPlan {
  def limit(plan: LogicalPlan,
            rel: OlapLimitRel,
            dataContext: DataContext): LogicalPlan = {
    val visitor = new SparderRexVisitor(plan,
      rel.getInput.getRowType,
      dataContext)
    val limit = BigDecimal(rel.getLocalFetch.accept(visitor).toString).toInt

    if (rel.getLocalOffset != null) {
      val offset = BigDecimal(rel.getLocalOffset.accept(visitor).toString).toInt
      val offsetPlan = Offset(Literal(offset), plan)
      Limit(Literal(limit), offsetPlan)
    } else {
      Limit(Literal(limit), plan)
    }
  }
}
