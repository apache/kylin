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

package org.apache.kylin.engine.spark.job

import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.metadata.cube.model.NBatchConstants.{P_DATAFLOW_ID, P_LAYOUT_IDS}
import org.apache.kylin.metadata.cube.model.{NDataLayoutDetails, NDataLayoutDetailsManager, NDataflow, NDataflowManager}

import scala.collection.JavaConverters._

abstract class AbstractLayoutDataOptimizeJob extends SparkApplication {

  lazy val dataFlow: NDataflow = NDataflowManager.getInstance(config, project).getDataflow(getParam(P_DATAFLOW_ID))
  lazy val layoutDetailsManager: NDataLayoutDetailsManager = NDataLayoutDetailsManager.getInstance(config, project);

  def getLayoutDetails(): List[NDataLayoutDetails] = {
    NSparkCubingUtil.str2Longs(getParam(P_LAYOUT_IDS))
      .asScala.map(layoutDetailsManager.getNDataLayoutDetails(dataFlow.getId, _)).toList
  }

  def getColNameByIdentity(identity: String): String = {
    this.dataFlow.getModel.getColumnIdByColumnName(identity).toString
  }

  def getColNameByIdentityWithBackTick(identity: String): String = {
    val colName = getColNameByIdentity(identity)
    s"`$colName`"
  }
}
