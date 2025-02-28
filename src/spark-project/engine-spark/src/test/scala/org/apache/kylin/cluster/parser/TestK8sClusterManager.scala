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

package org.apache.kylin.cluster.parser

import io.fabric8.kubernetes.api.model.{Quantity, ResourceQuota, ResourceQuotaStatus}
import io.fabric8.volcano.scheduling.v1beta1.{Queue, QueueSpec}
import org.apache.kylin.cluster.K8sClusterManager
import org.apache.spark.sql.common.SparderBaseFunSuite

import java.util

class TestK8sClusterManager extends SparderBaseFunSuite {

  test("getAvailableResourceByVolcano") {
    val clusterManager = new K8sClusterManager
    val queue = new Queue()
    val spec = new QueueSpec()
    queue.setSpec(spec)
    val capability = new util.HashMap[String, Quantity]()
    spec.setCapability(capability)
    var res = clusterManager.getAvailableResourceByVolcano(queue)
    assert(res.available.vCores == 1000)
    capability.put("cpu", Quantity.parse("100000m"))
    capability.put("memory", Quantity.parse("10240Mi"))
    res = clusterManager.getAvailableResourceByVolcano(queue)
    assert(res.available.vCores == 100)
    assert(res.available.memory == 10 * 1024)
  }

  test("getAvailableResourceByQuota") {
    val clusterManager = new K8sClusterManager
    val list = new util.ArrayList[ResourceQuota]();
    val quota = new ResourceQuota
    val status = new ResourceQuotaStatus
    quota.setStatus(status)
    val hard = new util.HashMap[String, Quantity]()
    status.setHard(hard)
    val used = new util.HashMap[String, Quantity]()
    status.setUsed(used)
    list.add(quota)

    var res = clusterManager.getAvailableResourceByQuota(list)
    assert(res.available.vCores == 1000)

    hard.put("limits.cpu", Quantity.parse("100"))
    hard.put("limits.memory", Quantity.parse("10240Mi"))
    used.put("limits.memory", Quantity.parse("1Gi"))
    res = clusterManager.getAvailableResourceByQuota(list)
    assert(res.max.vCores == 100)
    assert(res.available.vCores == 100)
    assert(res.max.memory == 10 * 1024)
    assert(res.available.memory == 9 * 1024)
  }
}
