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

import org.apache.kylin.softaffinity.SoftAffinityManager
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.FSNamespaceUtils
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SQLConf, SessionState}

/**
 * hive session  hava some rule exp: find datasource table rule
 */
class KylinHiveSessionStateBuilder(sparkSession: SparkSession,
                                   parentState: Option[SessionState] = None)
  extends HiveSessionStateBuilder(sparkSession, parentState) {

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

  override protected def newBuilder: NewBuilder =
    new KylinHiveSessionStateBuilder(_, _)

}

object Const {
  val SoftAffinityInputFormatMap: Map[String, String] = Map(
    "org.apache.hadoop.mapred.TextInputFormat" -> "org.apache.kylin.cache.softaffinity.SoftAffinityTextInputFormat"
  )
  val SerDeMap: Map[String, String] = Map(
    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" -> "io.kyligence.hive.serde2.lazy.LazyQuoteAwareSerDe"
  )
}

case class HiveStorageRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  var forceDisableSoftAffinity = false

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case relation: HiveTableRelation
      if DDLUtils.isHiveTable(relation.tableMeta) =>

      // replace file system
      val specFS = sparkSession.sessionState.conf.getConf(SQLConf.HIVE_SPECIFIC_FS_LOCATION)
      var specStorage = FSNamespaceUtils.replaceLocWithSpecPrefix(specFS, relation.tableMeta.storage)

      // replace input format, for soft affinity during hive pushdown
      val softAffinityEnabled = !forceDisableSoftAffinity && SoftAffinityManager.usingSoftAffinity && SoftAffinityManager.usingSoftAffinityForHivePushdown
      val oldInputFormat = relation.tableMeta.storage.inputFormat
      val newInputFormat = if (oldInputFormat.isEmpty || !softAffinityEnabled) {
        oldInputFormat
      } else {
        Option(Const.SoftAffinityInputFormatMap.getOrElse(oldInputFormat.get, oldInputFormat.get))
      }

      // replace serde, for CSV quote support
      val oldSerDe = relation.tableMeta.storage.serde
      val newSerDe = if (oldSerDe.isEmpty || !KylinConfig.getInstanceFromEnv.isSupportPushdownHiveCsvEnhancement) {
        oldSerDe
      } else {
        Option(Const.SerDeMap.getOrElse(oldSerDe.get, oldSerDe.get))
      }

      specStorage = specStorage.copy(inputFormat = newInputFormat, serde = newSerDe)
      val specTableMeta = relation.tableMeta.copy(storage = specStorage)
      val specRelation = relation.copy(tableMeta = specTableMeta)
      specRelation
  }

  def setForceDisableSoftAffinity(value: Boolean): Unit = {
    forceDisableSoftAffinity = value
  }
}

/**
 * use for no hive mode
 */
class KylinSessionStateBuilder(sparkSession: SparkSession,
                               parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(sparkSession, parentState) {

  override protected def newBuilder: NewBuilder =
    new KylinSessionStateBuilder(_, _)

}
