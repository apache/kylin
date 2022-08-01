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
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}

class SparkPrometheusMetricsTest extends SparderBaseFunSuite with SharedSparkSession {

  test("fetchExecutorMetricsInfo") {
    val str = SparkPrometheusMetrics.fetchExecutorMetricsInfo("applicationId")
    assert(str == "")
  }

  test("fetchDriverMetricsInfo") {
    val str = SparkPrometheusMetrics.fetchDriverMetricsInfo("applicationId")
    assert(str == "")
  }

  test("getExecutorsUrl") {
    val str = SparkPrometheusMetrics.getExecutorsUrl("localhost", 7070, "applicationId", false)
    assert(str == "http://localhost:7070/proxy/applicationId/metrics/executors/prometheus")

    val str1 = SparkPrometheusMetrics.getExecutorsUrl("localhost", 7070, "applicationId", true)
    assert(str1 == "https://localhost:7070/proxy/applicationId/metrics/executors/prometheus")
  }

  test("getDriverUrl") {
    val str = SparkPrometheusMetrics.getDriverUrl("localhost", 7070, "applicationId", false)
    assert(str == "http://localhost:7070/proxy/applicationId/metrics/prometheus")

    val str1 = SparkPrometheusMetrics.getDriverUrl("localhost", 7070, "applicationId", true)
    assert(str1 == "https://localhost:7070/proxy/applicationId/metrics/prometheus")
  }

  test("getSocketAddress") {
    val yarnConfiguration = StorageUtils.getCurrentYarnConfiguration
    val yarnAddress = SparkPrometheusMetrics.getSocketAddress(yarnConfiguration)
    assert(yarnAddress.head._2 == 8088)

    yarnConfiguration.set("yarn.http.policy", "HTTPS_ONLY")
    val yarnAddress1 = SparkPrometheusMetrics.getSocketAddress(yarnConfiguration)
    assert(yarnAddress1.head._2 == 8090)

    yarnConfiguration.set("yarn.resourcemanager.ha.enabled", "true")
    yarnConfiguration.set("yarn.resourcemanager.ha.rm-ids", "1,2")
    yarnConfiguration.set("yarn.resourcemanager.webapp.https.address.1", "localhost1:8010")
    yarnConfiguration.set("yarn.resourcemanager.webapp.https.address.2", "localhost2:8011")

    val yarnAddress2 = SparkPrometheusMetrics.getSocketAddress(yarnConfiguration)
    assert(yarnAddress2.size == 2)

    yarnConfiguration.set("yarn.http.policy", "HTTP_ONLY")
    yarnConfiguration.set("yarn.resourcemanager.ha.rm-ids", "1,2")
    yarnConfiguration.set("yarn.resourcemanager.webapp.address.1", "localhost1:8010")
    yarnConfiguration.set("yarn.resourcemanager.webapp.address.2", "localhost2:8011")

    val yarnAddress3 = SparkPrometheusMetrics.getSocketAddress(yarnConfiguration)
    assert(yarnAddress3.size == 2)
  }
}
