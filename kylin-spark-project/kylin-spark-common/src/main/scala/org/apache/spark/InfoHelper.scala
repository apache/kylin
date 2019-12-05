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

package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{JobData, StageData}


class InfoHelper(val spark: SparkSession) {
  private lazy val store: AppStatusStore = spark.sparkContext.statusStore

  def getJobsByGroupId(id: String): Seq[JobData] = {
    if (id != null) {
      store.jobsList(statuses = null).filter(_.jobGroup.exists(_.endsWith(id)))
    } else {
      store.jobsList(statuses = null)
    }
  }

  def getStagesWithDetailsByStageIds(stageIds: Seq[Int]): Seq[StageData] = {
    stageIds.flatMap(stageId => store.stageData(stageId, details = true))
  }
}
