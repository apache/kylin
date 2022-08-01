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

package org.apache.kylin.rest.discovery;

import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum;
import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum.ALL;
import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum.JOB;
import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum.QUERY;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.cloud.zookeeper.discovery.ZookeeperInstance;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;

@ConditionalOnZookeeperEnabled
@Component
public class KylinServiceDiscoveryCache implements KylinServiceDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(KylinServiceDiscoveryCache.class);
    private static final Callback UPDATE_ALL_EPOCHS = () -> {
        try {
            EpochManager.getInstance().updateAllEpochs();
        } catch (Exception e) {
            logger.error("UpdateAllEpochs failed", e);
        }
    };
    private final Map<ServerModeEnum, ServiceCache<ZookeeperInstance>> serverModeCacheMap;
    private final List<ServerModeEnum> ALL_CHECK_MODE_LIST = ImmutableList.of(ALL, JOB, QUERY);
    @Autowired
    private KylinServiceDiscoveryClient kylinServiceDiscoveryClient;
    @Autowired
    private ServiceDiscovery<ZookeeperInstance> serviceDiscovery;
    @Autowired
    private CuratorFramework curatorClient;

    public KylinServiceDiscoveryCache() {
        serverModeCacheMap = Maps.newHashMap();
    }

    private static String instance2ServerStr(@Nonnull ServiceInstance<ZookeeperInstance> serviceInstance) {
        Preconditions.checkNotNull(serviceInstance, "service instance is null");

        return serviceInstance.getAddress() + ":" + serviceInstance.getPort();
    }

    @PostConstruct
    private void init() throws Exception {
        registerServiceCache();
        startServiceCache();
    }

    @PreDestroy
    private void close() {
        for (ServiceCache<ZookeeperInstance> serviceCache : serverModeCacheMap.values()) {
            IOUtils.closeQuietly(serviceCache);
        }
        serverModeCacheMap.clear();
    }

    private void registerServiceCache() throws Exception {
        for (ServerModeEnum serverModeEnum : ALL_CHECK_MODE_LIST) {
            registerServiceCacheByMode(serverModeEnum);
        }
    }

    private void registerServiceCacheByMode(ServerModeEnum modeEnum) throws Exception {
        switch (modeEnum) {
        case QUERY:
            serverModeCacheMap.put(QUERY, createServiceCache(serviceDiscovery, QUERY, () -> {
            }));
            break;
        case JOB:
            serverModeCacheMap.put(JOB, createServiceCache(serviceDiscovery, JOB, UPDATE_ALL_EPOCHS));
            break;
        case ALL:
            serverModeCacheMap.put(ALL, createServiceCache(serviceDiscovery, ALL, UPDATE_ALL_EPOCHS));
            break;
        default:
            break;
        }

    }

    private void startServiceCache() throws Exception {
        for (ServiceCache<ZookeeperInstance> serviceCache : serverModeCacheMap.values()) {
            serviceCache.start();
        }
    }

    private ServiceCache<ZookeeperInstance> createServiceCache(ServiceDiscovery<ZookeeperInstance> serviceDiscovery,
            ServerModeEnum serverMode, Callback action) throws Exception {

        //create mode path first
        createZkNodeIfNeeded(getZkPathByModeEnum(serverMode));

        ServiceCache<ZookeeperInstance> serviceCache = serviceDiscovery.serviceCacheBuilder().name(serverMode.getName())
                .threadFactory(Executors.defaultThreadFactory()).build();

        serviceCache.addListener(new ServiceCacheListener() {
            @Override
            public void cacheChanged() {
                List<String> serverNodes = getServerStrByServerMode(serverMode);
                Unsafe.setProperty("kylin.server.cluster-mode-" + serverMode.getName(),
                        StringUtils.join(serverNodes, ","));
                logger.info("kylin.server.cluster-mode-{} update to {}", serverMode.getName(), serverNodes);

                // current node is active all/job nodes, try to update all epochs
                if (getServerInfoByServerMode(JOB).stream().map(ServerInfoResponse::getHost).anyMatch(
                        server -> Objects.equals(server, kylinServiceDiscoveryClient.getLocalServiceServer()))) {
                    logger.debug("Current node is active node, try to update all epochs");
                    action.action();
                }
            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                logger.info("zookeeper connection state changed to {}", connectionState);
            }
        });

        return serviceCache;
    }

    private ServiceCache<ZookeeperInstance> getServiceCacheByMode(@Nonnull ServerModeEnum serverModeEnum) {
        Preconditions.checkNotNull(serverModeEnum, "server mode is null");

        val serviceCache = serverModeCacheMap.get(serverModeEnum);

        Preconditions.checkNotNull(serviceCache, "cannot find the server cache :" + serverModeEnum.getName());

        return serviceCache;
    }

    private List<String> getServerStrByServerMode(@Nonnull ServerModeEnum serverModeEnum) {
        Preconditions.checkNotNull(serverModeEnum, "server mode is null!");

        return getServiceCacheByMode(serverModeEnum).getInstances().stream()
                .map(KylinServiceDiscoveryCache::instance2ServerStr).collect(Collectors.toList());
    }

    @Override
    public List<ServerInfoResponse> getServerInfoByServerMode(@Nullable ServerModeEnum... serverModeEnums) {
        List<ServerInfoResponse> serverInfoResponses = Lists.newArrayList();
        if (ArrayUtils.isEmpty(serverModeEnums)) {
            return serverInfoResponses;
        }

        for (ServerModeEnum serverModeEnum : serverModeEnums) {
            serverInfoResponses.addAll(getServiceCacheByMode(serverModeEnum).getInstances().stream()
                    .map(serviceIns -> new ServerInfoResponse(instance2ServerStr(serviceIns), serverModeEnum.getName()))
                    .collect(Collectors.toList()));
        }

        return serverInfoResponses;
    }

    private void createZkNodeIfNeeded(String nodePath) throws Exception {
        try {
            if (curatorClient.checkExists().forPath(nodePath) != null) {
                logger.warn("The znode {} is existed", nodePath);
                return;
            }
            curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(nodePath);
            logger.info("create znode {} success", nodePath);

        } catch (KeeperException.NodeExistsException e) {
            logger.warn("The znode {} has been created by others", nodePath);
        } catch (Exception e) {
            logger.error("Fail to check or create znode for {}", nodePath, e);
            throw e;
        }
    }
}
