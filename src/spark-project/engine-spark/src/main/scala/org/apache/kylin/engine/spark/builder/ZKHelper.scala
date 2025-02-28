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

package org.apache.kylin.engine.spark.builder

import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Try zookeeper jaas config for distributed lock
 * [[org.apache.kylin.common.lock.curator.CuratorDistributedLockFactory]]
 */
object ZKHelper extends Logging {
  private val YARN_CLUSTER: String = "cluster"

  def tryZKJaasConfiguration(ss: SparkSession): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    if (YARN_CLUSTER.equals(config.getDeployMode)
      && config.getDistributedLockFactoryFullClassName.contains("CuratorDistributedLockFactory")) {
      val kapConfig = KapConfig.wrap(config)
      if (KapConfig.FI_PLATFORM.equals(kapConfig.getKerberosPlatform) || KapConfig.TDH_PLATFORM.equals(kapConfig.getKerberosPlatform)) {
        val sparkConf = ss.sparkContext.getConf
        val principal = sparkConf.get("spark.kerberos.principal")
        val keytab = sparkConf.get("spark.kerberos.keytab")
        logInfo(s"ZKJaasConfiguration principal: $principal, keyTab: $keytab")
        javax.security.auth.login.Configuration.setConfiguration(new ZKJaasConfiguration(principal, keytab))
      }
    }
  }

}
