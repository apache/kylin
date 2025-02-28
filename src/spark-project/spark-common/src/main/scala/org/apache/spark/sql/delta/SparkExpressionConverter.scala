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
package org.apache.spark.sql.delta

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.DataSourceStrategy

import io.delta.standalone.expressions.{Expression => SExpression}
import io.delta.standalone.types.StructType

object SparkExpressionConverter {

  def convertToStandaloneExpr(schema: StructType, sparkExpression: Expression): SExpression = {
    SparkFilters.convert(schema,
      DataSourceStrategy.translateFilter(sparkExpression, supportNestedPredicatePushdown = true).get)
  }

  def convertToStandaloneExpr(schema: StructType, sparkExpressions: Seq[Expression]): SExpression = {
    val filters = sparkExpressions.flatMap(expr =>
      DataSourceStrategy.translateFilter(expr, supportNestedPredicatePushdown = true))
    SparkFilters.convert(schema, filters)
  }
}
