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

package org.apache.spark.metrics

import org.apache.kylin.engine.spark.utils.StorageUtils
import org.apache.hadoop.yarn.conf.{HAUtil, YarnConfiguration}
import org.apache.http.client.config.RequestConfig
import org.apache.spark.internal.Logging
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object SparkPrometheusMetrics extends Logging {

  def fetchExecutorMetricsInfo(applicationId: String): String = {
    val yarnConfiguration = StorageUtils.getCurrentYarnConfiguration
    getMetricsByUrlsWithHa(getSocketAddress(yarnConfiguration)
      .map(address => getExecutorsUrl(address._1, address._2, applicationId, YarnConfiguration.useHttps(yarnConfiguration))))
  }

  def fetchDriverMetricsInfo(applicationId: String): String = {
    val yarnConfiguration = StorageUtils.getCurrentYarnConfiguration
    getMetricsByUrlsWithHa(getSocketAddress(yarnConfiguration)
      .map(address => getDriverUrl(address._1, address._2, applicationId, YarnConfiguration.useHttps(yarnConfiguration))))
  }

  def getExecutorsUrl(hostName: String, port: Int, appId: String, useHttps: Boolean): String = {
    if (useHttps) {
      s"https://$hostName:$port/proxy/$appId/metrics/executors/prometheus"
    } else {
      s"http://$hostName:$port/proxy/$appId/metrics/executors/prometheus"
    }
  }

  def getDriverUrl(hostName: String, port: Int, appId: String, useHttps: Boolean): String = {
    if (useHttps) {
      s"https://$hostName:$port/proxy/$appId/metrics/prometheus"
    } else {
      s"http://$hostName:$port/proxy/$appId/metrics/prometheus"
    }
  }

  private def getMetricsByUrlsWithHa(urls: Iterable[String]): String = {
    urls.foreach { url =>
      try {
        return getResponse(url)
      } catch {
        case ex: Exception => logError(ex.getMessage, ex)
      }
    }
    ""
  }

  def getSocketAddress(conf: YarnConfiguration): Map[String, Int] = {
    val useHttps = YarnConfiguration.useHttps(conf)
    val addresses = if (HAUtil.isHAEnabled(conf)) {
      val haIds = HAUtil.getRMHAIds(conf).toArray
      require(haIds.nonEmpty, "Ha ids is empty, please check your yarn-site.xml.")
      if (useHttps) {
        haIds.map(id => conf.getSocketAddr(s"${YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS}.$id",
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_HTTPS_PORT))
      } else {
        haIds.map(id => conf.getSocketAddr(s"${YarnConfiguration.RM_WEBAPP_ADDRESS}.$id",
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_PORT))
      }
    } else {
      if (useHttps) {
        Array(conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_HTTPS_PORT))
      } else {
        Array(conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_PORT))
      }
    }
    addresses.map(address => address.getHostName -> address.getPort).toMap
  }

  def getResponse(url: String, timeout: Int): String = {
    val httpClient = HttpClients.createDefault()
    try {
      val httpGet = new HttpGet(url)
      httpGet.setConfig(RequestConfig.custom().setConnectTimeout(timeout).setSocketTimeout(timeout).build())
      val response = httpClient.execute(httpGet)
      if (response.getStatusLine.getStatusCode != 200) {
        throw new RuntimeException(s"spark3 prometheus metrics request $url fail, response : " + EntityUtils.toString(response.getEntity))
      }
      EntityUtils.toString(response.getEntity)
    } finally {
      httpClient.close()
    }
  }

  def getResponse(url: String): String = {
    getResponse(url, 5000)
  }
}
