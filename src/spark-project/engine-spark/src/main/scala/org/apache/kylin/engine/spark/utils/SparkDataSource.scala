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

package org.apache.kylin.engine.spark.utils

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.NSparkCubingEngine
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.kylin.metadata.model.TableDesc
import org.apache.kylin.source.SourceFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkDataSource {

  implicit class SparkSource(sparkSession: SparkSession) {
    def table(tableDesc: TableDesc): DataFrame = {
      val params = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv)
        .getProject(tableDesc.getProject).getLegalOverrideKylinProps
      SourceFactory
        .createEngineAdapter(tableDesc,
          classOf[NSparkCubingEngine.NSparkCubingSource])
        .getSourceData(tableDesc, sparkSession, params)
    }

    def table(tableDesc: TableDesc, segmentStart: String, segmentEnd: String): DataFrame = {
      val params = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv)
        .getProject(tableDesc.getProject).getLegalOverrideKylinProps
      params.put("segmentStart", segmentStart)
      params.put("segmentEnd", segmentEnd)
      SourceFactory
        .createEngineAdapter(tableDesc,
          classOf[NSparkCubingEngine.NSparkCubingSource])
        .getSourceData(tableDesc, sparkSession, params)
    }
  }

}
