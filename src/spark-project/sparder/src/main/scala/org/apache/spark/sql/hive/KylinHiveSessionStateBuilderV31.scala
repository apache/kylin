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
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState}

/**
 * hive session  hava some rule exp: find datasource table rule
 *
 * @param sparkSession
 * @param parentState
 */
class KylinHiveSessionStateBuilderV31(sparkSession: SparkSession,
                                   parentState: Option[SessionState] = None)
    extends HiveSessionStateBuilder(sparkSession, parentState, Map.empty) {

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

  override protected def newBuilder: NewBuilder =
    new KylinHiveSessionStateBuilderV31(_, _)

}

/**
 * use for no hive mode
 *
 * @param sparkSession
 * @param parentState
 */
class KylinSessionStateBuilderV31(sparkSession: SparkSession,
                               parentState: Option[SessionState] = None)
    extends BaseSessionStateBuilder(sparkSession, parentState, Map.empty) {

  override protected def newBuilder: NewBuilder =
    new KylinSessionStateBuilderV31(_, _)

}
