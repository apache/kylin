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

import java.util.Objects
import org.apache.kylin.guava30.shaded.common.collect.Maps

import javax.security.auth.login.AppConfigurationEntry
import org.apache.hadoop.security.authentication.util.KerberosUtil
import org.apache.kylin.common.util.Unsafe
import org.apache.zookeeper.client.ZooKeeperSaslClient

class ZKJaasConfiguration(private val principal: String,
                          private val keyTab: String) extends javax.security.auth.login.Configuration {

  private val TRUE: String = "true"
  private val FALSE: String = "false"
  private val DEFAULT_CONTEXT_NAME = "Client"

  Unsafe.setProperty("zookeeper.sasl.client", TRUE)

  private val contextName = System.getProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, DEFAULT_CONTEXT_NAME)

  private def getSpecifiedOptions: java.util.Map[String, String] = {
    val options = Maps.newHashMap[String, String]()
    val vendor = System.getProperty("java.vendor")
    val isIBM = if (Objects.isNull(vendor)) {
      false
    } else {
      vendor.contains("IBM")
    }
    if (isIBM) {
      options.put("credsType", "both")
    } else {
      options.put("useKeyTab", TRUE)
      options.put("useTicketCache", FALSE)
      options.put("doNotPrompt", TRUE)
      options.put("storeKey", TRUE)
    }

    if (Objects.nonNull(keyTab)) {
      if (isIBM) {
        options.put("useKeytab", keyTab)
      } else {
        options.put("keyTab", keyTab)
        options.put("useKeyTab", TRUE)
        options.put("useTicketCache", FALSE)
      }
    }
    options.put("principal", principal)
    options
  }

  private def getSpecifiedEntry: Array[AppConfigurationEntry] = {
    val array = new Array[AppConfigurationEntry](1)
    array(0) = new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName, //
      AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, getSpecifiedOptions)
    array
  }

  override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
    if (name.equals(contextName)) {
      return getSpecifiedEntry
    }
    try {
      val conf = javax.security.auth.login.Configuration.getConfiguration
      conf.getAppConfigurationEntry(name)
    } catch {
      case _: SecurityException => null
    }
  }
}
