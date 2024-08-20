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
package org.apache.spark.sql.common

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.GlutenSessionExtensions
import org.apache.gluten.test.FallbackUtil
import org.apache.gluten.test.FallbackUtil.collectWithSubqueries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

object GlutenTestUtil {

  def hasFallback(plan: SparkPlan): Boolean = FallbackUtil.hasFallback(plan)

  def hasFallbackOnStep[T <: SparkPlan](plan: SparkPlan, clazz: Class[T]): Boolean = {
    var fallbackOperator: Seq[SparkPlan] = null
    if (plan.isInstanceOf[AdaptiveSparkPlanExec]) {
      fallbackOperator = collectWithSubqueries(plan) {
        case plan if !plan.isInstanceOf[GlutenPlan] && !FallbackUtil.skip(plan) =>
          plan
      }
    } else {
      fallbackOperator = plan.collectWithSubqueries {
        case plan if !plan.isInstanceOf[GlutenPlan] && !FallbackUtil.skip(plan) =>
          plan
      }
    }
    if (fallbackOperator.nonEmpty && fallbackOperator.map(_.getClass).contains(clazz)) {
      true
    } else {
      false
    }
  }

  def glutenEnabled(spark: SparkSession): Boolean = {
    spark.conf.get("spark.plugins", "").contains("GlutenPlugin") &&
      spark.conf.get(
        GlutenConfig.GLUTEN_ENABLED_KEY,
        GlutenConfig.GLUTEN_ENABLED_BY_DEFAULT.toString).toBoolean && glutenEnabledForCurrentThread(spark)
  }

  private def glutenEnabledForCurrentThread(spark: SparkSession): Boolean = {
    val enabled =
      spark.sparkContext.getLocalProperty(GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY)
    if (enabled != null) {
      enabled.toBoolean
    } else {
      true
    }
  }
}
