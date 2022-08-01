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
import org.apache.kylin.query.relnode.KapFilterRel
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.spark.sql.{Column, DataFrame}

object FilterPlan extends LogEx {
  def filter(
              inputs: java.util.List[DataFrame],
              rel: KapFilterRel,
              dataContext: DataContext): DataFrame = logTime("filter", debug = true) {
      val df = inputs.get(0)
      val visitor = new SparderRexVisitor(df,
        rel.getInput.getRowType,
        dataContext)
      val filterColumn = rel.getCondition.accept(visitor).asInstanceOf[Column]
      df.filter(filterColumn)
  }
}
