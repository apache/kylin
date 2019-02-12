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

package org.apache.kylin.common.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ZKBasedServerDiscovery implements ServerDiscovery {
  private static final Logger logger = LoggerFactory.getLogger(ZKBasedServerDiscovery.class);
  private static ZKBasedServerDiscovery INSTANCE = null;

  public static ZKBasedServerDiscovery getInstance() throws Exception {
    if (INSTANCE == null) {
      INSTANCE = createInstance();
    }
    return INSTANCE;
  }

  public synchronized static ZKBasedServerDiscovery createInstance() throws Exception  {
    destroyInstance();
    INSTANCE = new ZKBasedServerDiscovery(KylinConfig.getInstanceFromEnv());
    return INSTANCE;
  }

  public synchronized static void destroyInstance() {
    ZKBasedServerDiscovery tmp = INSTANCE;
    INSTANCE = null;
    if (tmp != null) {
      try {
        tmp.close();
      } catch (Exception e) {
        logger.error("error stop ZKBasedServerDiscovery", e);
        throw new RuntimeException(e);
      }
    }
  }

  private CuratorFramework client;
  private ServiceDiscovery<KylinInstanceDetail> serviceDiscovery;
  private ServiceInstance<KylinInstanceDetail> instance;

  public ZKBasedServerDiscovery(KylinConfig config) throws Exception {
    client = CuratorFrameworkFactory.newClient(config.getZookeeperConnectString(),
        new ExponentialBackoffRetry(1000, 3));
    client.start();

    JsonInstanceSerializer<KylinInstanceDetail> serializer = new JsonInstanceSerializer<KylinInstanceDetail>(KylinInstanceDetail.class);
    serviceDiscovery = ServiceDiscoveryBuilder.builder(KylinInstanceDetail.class)
        .client(client)
        .serializer(serializer)
        .basePath(config.getZookeeperBasePath())
        .build();

    String host = InetAddress.getLocalHost().getHostName();
    int port = config.getServerPort();

    instance = ServiceInstance.<KylinInstanceDetail>builder()
        .name("servers")
        .address(host)
        .port(port)
        .payload(new KylinInstanceDetail(UUID.randomUUID().toString(), host + ":" + port))
        .build();
  }

  @Override
  public boolean registerService() throws Exception {
    serviceDiscovery.registerService(instance);
    logger.info("Register kylin server: " + instance);
    return true;
  }

  @Override
  public boolean unregisterService() throws Exception {
    serviceDiscovery.unregisterService(instance);
    logger.info("Unregister kylin server: " + instance);
    return true;
  }

  @Override
  public List<String> getServers() throws Exception {
    List<String> servers = new ArrayList<String>();
    for (ServiceInstance<KylinInstanceDetail> instance: serviceDiscovery.queryForInstances("servers")) {
      servers.add(instance.getPayload().getListenAddress());
    }
    logger.info("Get kylin servers: " + servers);
    return servers;
  }

  public void close() {
    try {
      serviceDiscovery.close();
      client.close();
    } catch (Exception e) {
      logger.error("Service discovery shutdown failed~", e);
    }
  }
}
