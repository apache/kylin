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

import org.apache.kylin.cluster.{ClusterManagerFactory, IClusterManager}
import org.apache.kylin.common.KylinConfig
import org.apache.spark.SparkConf

import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.NotThreadSafe

class KylinBuildEnv(config: KylinConfig) {

  val buildJobInfos: BuildJobInfos = new BuildJobInfos

  lazy val sparkConf: SparkConf = new SparkConf()

  val kylinConfig: KylinConfig = config

  lazy val clusterManager: IClusterManager = ClusterManagerFactory.create(config)

}

object KylinBuildEnv {
  private val defaultEnv = new AtomicReference[KylinBuildEnv]

  @NotThreadSafe
  def getOrCreate(config: KylinConfig): KylinBuildEnv = {
    if (defaultEnv.get() == null) {
      val env = new KylinBuildEnv(config)
      defaultEnv.set(env)
      env
    } else {
      defaultEnv.get()
    }
  }

  def get(): KylinBuildEnv = {
    defaultEnv.get()
  }

  def clean(): Unit = {
    defaultEnv.set(null)
  }
}
