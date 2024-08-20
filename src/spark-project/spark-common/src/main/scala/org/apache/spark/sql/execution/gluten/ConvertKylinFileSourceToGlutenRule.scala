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

package org.apache.spark.sql.execution.gluten

import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenPlan, ValidatablePlan}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.utils.PushDownUtil
import org.apache.spark.sql.execution.{KylinFileSourceScanExec, LayoutFileSourceScanExec, SparkPlan}

class ConvertKylinFileSourceToGlutenRule(val session: SparkSession) extends Rule[SparkPlan] {

  private def tryReturnGlutenPlan(glutenPlan: GlutenPlan, originPlan: SparkPlan): SparkPlan = {
    glutenPlan match {
      case plan: ValidatablePlan if plan.doValidate().ok() =>
        logDebug(s"Columnar Processing for ${originPlan.getClass} is currently supported.")
        glutenPlan
      case _ =>
        logDebug(s"Columnar Processing for ${originPlan.getClass} is currently unsupported.")
        originPlan
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case f: KylinFileSourceScanExec =>

      // convert to Gluten transformer
      val transformer = new KylinFileSourceScanExecTransformer(
        f.relation,
        f.output,
        f.requiredSchema,
        f.partitionFilters,
        None,
        f.optionalShardSpec,
        f.optionalNumCoalescedBuckets,
        PushDownUtil.removeNotSupportPushDownFilters(f.conf, f.output, f.dataFilters),
        f.tableIdentifier,
        f.disableBucketedScan,
        f.sourceScanRows
      )
      // Transformer validate
      tryReturnGlutenPlan(transformer, f)

    case l: LayoutFileSourceScanExec =>
      // convert to Gluten transformer
      val transformer = new FileSourceScanExecTransformer(
        l.relation,
        l.output,
        l.requiredSchema,
        l.partitionFilters,
        l.optionalBucketSet,
        l.optionalNumCoalescedBuckets,
        PushDownUtil.removeNotSupportPushDownFilters(l.conf, l.output, l.dataFilters),
        l.tableIdentifier,
        l.disableBucketedScan
      )
      // Transformer validate
      tryReturnGlutenPlan(transformer, l)
  }
}
