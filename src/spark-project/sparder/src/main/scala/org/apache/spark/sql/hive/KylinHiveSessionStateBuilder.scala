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
package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.FSNamespaceUtils
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SQLConf, SessionState}

/**
 * hive session  hava some rule exp: find datasource table rule
 *
 * @param sparkSession
 * @param parentState
 */
class KylinHiveSessionStateBuilder(sparkSession: SparkSession,
                                   parentState: Option[SessionState] = None)
    extends HiveSessionStateBuilder(sparkSession, parentState) {

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

  override protected def newBuilder: NewBuilder =
    new KylinHiveSessionStateBuilder(_, _)

}

case class ReplaceLocationRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case relation: HiveTableRelation
      if DDLUtils.isHiveTable(relation.tableMeta) =>
      val specFS = sparkSession.sessionState.conf.getConf(SQLConf.HIVE_SPECIFIC_FS_LOCATION)
      val specCatalog = FSNamespaceUtils.replaceLocWithSpecPrefix(specFS,
        relation.tableMeta.storage)
      val specTableMeta = relation.tableMeta.copy(storage = specCatalog)
      val specRelation = if (specFS != null && specCatalog.locationUri.isDefined) {
        relation.copy(tableMeta = specTableMeta)
      } else relation
      specRelation
  }
}

/**
 * use for no hive mode
 *
 * @param sparkSession
 * @param parentState
 */
class KylinSessionStateBuilder(sparkSession: SparkSession,
                               parentState: Option[SessionState] = None)
    extends BaseSessionStateBuilder(sparkSession, parentState) {

  override protected def newBuilder: NewBuilder =
    new KylinSessionStateBuilder(_, _)

}
