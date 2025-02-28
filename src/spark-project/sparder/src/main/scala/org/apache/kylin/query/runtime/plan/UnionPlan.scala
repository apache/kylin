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
import org.apache.kylin.query.relnode.OlapUnionRel
import org.apache.spark.sql.SparkOperation
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, LogicalPlan, Union}
import org.apache.spark.sql.functions.col

object UnionPlan {
  def union(plans: Seq[LogicalPlan],
            rel: OlapUnionRel,
            dataContext: DataContext): LogicalPlan = {
    val unionPlan = if (rel.all) {
      Union(plans)
    } else {
      val uPlan = Union(plans)
      Deduplicate(plans.head.output, uPlan)
    }

    SparkOperation.project(plans.head.output.map(c => col(c.name)), unionPlan)
  }
}
