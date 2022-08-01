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

package org.apache.kylin.cluster

import org.apache.spark.sql.SparkSession

import java.util

trait IClusterManager {
  def fetchMaximumResourceAllocation: ResourceInfo

  def fetchQueueAvailableResource(queueName: String): AvailableResource

  def getBuildTrackingUrl(sparkSession: SparkSession): String

  def killApplication(jobStepId: String): Unit

  def killApplication(jobStepPrefix: String, jobStepId: String): Unit

  def isApplicationBeenKilled(applicationId: String): Boolean

  def getRunningJobs(queues: util.Set[String]): util.List[String]

  def fetchQueueStatistics(queueName: String): ResourceInfo

  def applicationExisted(jobId: String): Boolean

}

// memory unit is MB
case class ResourceInfo(memory: Int, vCores: Int) {

  def reduceMin(other: ResourceInfo): ResourceInfo = {
    ResourceInfo(Math.min(this.memory, other.memory), Math.min(this.vCores, other.vCores))
  }

  def percentage(percentage: Double): ResourceInfo = {
    ResourceInfo(Math.floor(this.memory * percentage).toInt, Math.floor(this.vCores * percentage).toInt)
  }
}

case class AvailableResource(available: ResourceInfo, max: ResourceInfo)
