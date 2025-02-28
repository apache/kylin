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

import org.apache.kylin.query.relnode.OlapValuesRel
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.JavaConverters._

object ValuesPlan {
  def values(rel: OlapValuesRel): LogicalPlan = {

    val schema = StructType(rel.getRowType.getFieldList.asScala.map { field =>
      StructField(
        field.getName,
        SparderTypeUtil.convertSqlTypeToSparkType(field.getType))
    })
    val output = schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

    val rows = rel.tuples.asScala.map { tp =>
      Row.fromSeq(tp.asScala.map(lit => SparderTypeUtil.getValueFromRexLit(lit)))
    }

    LocalRelation.fromExternalRows(output, rows)
  }
}
