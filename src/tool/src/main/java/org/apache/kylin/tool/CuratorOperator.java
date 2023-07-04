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
package org.apache.kylin.tool;

import static org.apache.kylin.common.ZookeeperConfig.geZKClientConnectionTimeoutMs;
import static org.apache.kylin.common.ZookeeperConfig.geZKClientSessionTimeoutMs;
import static org.apache.kylin.common.ZookeeperConfig.getZKConnectString;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.common.util.ZookeeperAclBuilder;
import org.apache.kylin.tool.discovery.ServiceInstanceSerializer;
import org.apache.kylin.tool.kerberos.KerberosLoginTask;
import org.apache.zookeeper.data.Stat;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CuratorOperator implements AutoCloseable {

    private CuratorFramework zkClient;

    public CuratorOperator() {
        KerberosLoginTask kerberosLoginTask = new KerberosLoginTask();
        kerberosLoginTask.execute();

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        String connectString = getZKConnectString();
        ZookeeperAclBuilder aclBuilder = new ZookeeperAclBuilder().invoke();
        zkClient = aclBuilder.setZKAclBuilder(CuratorFrameworkFactory.builder()).connectString(connectString)
                .sessionTimeoutMs(geZKClientSessionTimeoutMs()).connectionTimeoutMs(geZKClientConnectionTimeoutMs())
                .retryPolicy(retryPolicy).build();
        zkClient.start();
    }

    public static void main(String[] args) {
        int ret = 0;
        try (val curatorOperator = new CuratorOperator()) {
            if (curatorOperator.isJobNodeExist()) {
                ret = 1;
            }
        } catch (Exception e) {
            log.error("", e);
            ret = 1;
        }
        Unsafe.systemExit(ret);
    }

    public boolean isJobNodeExist() throws Exception {
        return checkNodeExist(ClusterConstant.ALL) || checkNodeExist(ClusterConstant.JOB);
    }

    private boolean checkNodeExist(String serverMode) throws Exception {
        String identifier = KylinConfig.getInstanceFromEnv().getMetadataUrlUniqueId();
        String nodePath = "/kylin/" + identifier + "/services/" + serverMode;
        Stat stat = zkClient.checkExists().forPath(nodePath);
        if (stat == null) {
            return false;
        }
        List<String> childNodes = zkClient.getChildren().forPath(nodePath);
        return childNodes != null && !childNodes.isEmpty();
    }

    @Override
    public void close() throws Exception {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    public String getAddress() throws Exception {
        String identifier = KylinConfig.getInstanceFromEnv().getMetadataUrlUniqueId();
        ServiceDiscovery serviceDiscovery = ServiceDiscoveryBuilder.builder(Object.class).client(zkClient)
                .basePath("/kylin/" + identifier + "/services")
                .serializer(new ServiceInstanceSerializer<>(Object.class)).build();
        serviceDiscovery.start();

        ServiceProvider provider = serviceDiscovery.serviceProviderBuilder().serviceName("all")
                .providerStrategy(new RandomStrategy<>()).build();
        provider.start();

        ServiceInstance serviceInstance = provider.getInstance();

        if (serviceInstance == null) {
            return null;
        }
        return serviceInstance.getAddress() + ":" + serviceInstance.getPort();
    }
}
