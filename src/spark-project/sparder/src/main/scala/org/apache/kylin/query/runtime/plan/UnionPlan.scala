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
import org.apache.kylin.query.relnode.KapUnionRel
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object UnionPlan {

  def union(
      inputs: java.util.List[DataFrame],
      rel: KapUnionRel,
      dataContext: DataContext): DataFrame = {
    var df = inputs.get(0)
    val drop = inputs.asScala.drop(1)
    if (rel.all) {
      for (other <- drop) {
        df = df.union(other)
      }
    } else {
      for (other <- drop) {
        df = df.union(other).distinct()
      }
    }
    df
  }
}
