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

package org.apache.kylin.job.impl.curator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.ServerMode;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.common.util.ZKUtil;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.lock.JobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class CuratorScheduler implements Scheduler<AbstractExecutable> {

    private static final Logger logger = LoggerFactory.getLogger(CuratorScheduler.class);
    private boolean started = false;
    private CuratorFramework curatorClient = null;
    private static CuratorLeaderSelector jobClient = null;
    private ServiceDiscovery<LinkedHashMap> serviceDiscovery = null;
    private ServiceCache<LinkedHashMap> serviceCache = null;
    private KylinConfig kylinConfig;
    private AtomicInteger count = new AtomicInteger();

    static final String JOB_ENGINE_LEADER_PATH = "/job_engine/leader";
    static final String KYLIN_SERVICE_PATH = "/service";
    static final String SERVICE_NAME = "kylin";
    static final String SERVICE_PAYLOAD_DESCRIPTION = "description";

    // the default constructor should exist for reflection initialization
    public CuratorScheduler() {

    }

    @VisibleForTesting
    CuratorScheduler(CuratorFramework curatorClient) {
        this.curatorClient = curatorClient;
    }

    @Override
    public void init(JobEngineConfig jobEngineConfig, JobLock jobLock) throws SchedulerException {
        kylinConfig = jobEngineConfig.getConfig();

        synchronized (this) {
            if (started) {
                logger.info("CuratorScheduler already started, skipped.");
                return;
            }

            // curatorClient can be assigned before only for test cases
            // due to creating independent curator client rather than share a cached one to avoid influences
            if (curatorClient == null) {
                curatorClient = ZKUtil.getZookeeperClient(kylinConfig);
            }

            final String serverMode = jobEngineConfig.getConfig().getServerMode();
            final String restAddress = kylinConfig.getServerRestAddress();
            try {
                registerInstance(restAddress, serverMode);
            } catch (Exception e) {
                throw new SchedulerException(e);
            }

            String jobEnginePath = JOB_ENGINE_LEADER_PATH;

            if (ServerMode.isJob(jobEngineConfig.getConfig())) {
                jobClient = new CuratorLeaderSelector(curatorClient, jobEnginePath, restAddress, jobEngineConfig);
                try {
                    logger.info("start Job Engine, lock path is: " + jobEnginePath);
                    jobClient.start();
                    monitorJobEngine();
                } catch (IOException e) {
                    throw new SchedulerException(e);
                }
            } else {
                logger.info("server mode: " + jobEngineConfig.getConfig().getServerMode() + ", no need to run job scheduler");
            }
            started = true;
        }
    }

    private void registerInstance(String restAddress, String mode) throws Exception {
        final String host = restAddress.substring(0, restAddress.indexOf(":"));
        final String port = restAddress.substring(restAddress.indexOf(":") + 1);

        final JsonInstanceSerializer<LinkedHashMap> serializer = new JsonInstanceSerializer<>(LinkedHashMap.class);
        final String servicePath = KYLIN_SERVICE_PATH;
        serviceDiscovery = ServiceDiscoveryBuilder.builder(LinkedHashMap.class).client(curatorClient)
                .basePath(servicePath).serializer(serializer).build();
        serviceDiscovery.start();

        serviceCache = serviceDiscovery.serviceCacheBuilder().name(SERVICE_NAME)
                .threadFactory(Executors.defaultThreadFactory()).build();

        serviceCache.addListener(new ServiceCacheListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            }

            @Override
            public void cacheChanged() {
                logger.info("Service discovery get cacheChanged notification");
                final List<ServiceInstance<LinkedHashMap>> instances = serviceCache.getInstances();
                final List<String> instanceNodes = Lists.transform(instances,
                        new Function<ServiceInstance<LinkedHashMap>, String>() {

                            @Nullable
                            @Override
                            public String apply(@Nullable ServiceInstance<LinkedHashMap> stringServiceInstance) {
                                return (String) stringServiceInstance.getPayload().get(SERVICE_PAYLOAD_DESCRIPTION);
                            }
                        });

                final String restServersInCluster = //
                        StringUtil.join(instanceNodes.stream().map(input -> { //
                            String[] split = input.split(":"); //
                            return split[0] + ":" + split[1]; //
                        }).collect(Collectors.toList()), ","); //


                logger.info("kylin.server.cluster-servers update to " + restServersInCluster);
                // update cluster servers
                System.setProperty("kylin.server.cluster-servers", restServersInCluster);

                // get servers and its mode(query, job, all)
                final String restServersInClusterWithMode = StringUtil.join(instanceNodes, ",");
                logger.info("kylin.server.cluster-servers-with-mode update to " + restServersInClusterWithMode);
                System.setProperty("kylin.server.cluster-servers-with-mode", restServersInClusterWithMode);
            }
        });
        serviceCache.start();

        final LinkedHashMap<String, String> instanceDetail = new LinkedHashMap<>();

        instanceDetail.put(SERVICE_PAYLOAD_DESCRIPTION, restAddress + ":" + mode);
        ServiceInstance<LinkedHashMap> thisInstance = ServiceInstance.<LinkedHashMap> builder().name(SERVICE_NAME)
                .payload(instanceDetail).port(Integer.valueOf(port)).address(host).build();

        for (ServiceInstance<LinkedHashMap> instance : serviceCache.getInstances()) {
            // Check for registered instances to avoid being double registered
            if (instance.getAddress().equals(thisInstance.getAddress())
                    && instance.getPort().equals(thisInstance.getPort())) {
                serviceDiscovery.unregisterService(instance);
            }
        }
        serviceDiscovery.registerService(thisInstance);
    }

    private void monitorJobEngine() {
        logger.info("Start collect monitor ZK Participants");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    boolean hasLeadership = jobClient.hasLeadership();
                    boolean hasDefaultSchedulerStarted = jobClient.hasDefaultSchedulerStarted();
                    if (!(hasLeadership == hasDefaultSchedulerStarted)) {
                        logger.error("Node(" + InetAddress.getLocalHost().getHostAddress()
                                + ") job server state conflict. Is ZK leader: " + hasLeadership
                                + "; Is active job server: " + hasDefaultSchedulerStarted);
                    }

                    if (count.incrementAndGet() == 10) {
                        logger.info("Current Participants: " + jobClient.getParticipants());
                        count.set(0);
                    }
                } catch (Throwable th) {
                    logger.error("Error when getting JVM info.", th);
                }
            }
        }, 3, kylinConfig.getInstanceFromEnv().getZKMonitorInterval(), TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() throws SchedulerException {
        IOUtils.closeQuietly(serviceCache);
        IOUtils.closeQuietly(serviceDiscovery);
        IOUtils.closeQuietly(curatorClient);
        IOUtils.closeQuietly(jobClient);
        started = false;
    }

    public static String slickMetadataPrefix(String metadataPrefix) {
        if (metadataPrefix.indexOf("/") >= 0) {
            // for local test
            if (metadataPrefix.endsWith("/")) {
                metadataPrefix = metadataPrefix.substring(0, metadataPrefix.length() - 2);
            }
            return metadataPrefix.substring(metadataPrefix.lastIndexOf("/") + 1);
        }

        return metadataPrefix;
    }

    @Override
    public boolean hasStarted() {
        return started;
    }

    public static CuratorLeaderSelector getLeaderSelector() {
        return jobClient;
    }

    static class JsonInstanceSerializer<T> implements InstanceSerializer<T> {
        private final ObjectMapper mapper;
        private final Class<T> payloadClass;
        private final JavaType type;

        JsonInstanceSerializer(Class<T> payloadClass) {
            this.payloadClass = payloadClass;
            this.mapper = new ObjectMapper();

            // to bypass https://issues.apache.org/jira/browse/CURATOR-394
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            this.type = this.mapper.getTypeFactory().constructType(ServiceInstance.class);
        }

        public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
            ServiceInstance rawServiceInstance = this.mapper.readValue(bytes, this.type);
            this.payloadClass.cast(rawServiceInstance.getPayload());
            return rawServiceInstance;
        }

        public byte[] serialize(ServiceInstance<T> instance) throws Exception {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            mapper.convertValue(instance.getPayload(), payloadClass);
            this.mapper.writeValue(out, instance);
            return out.toByteArray();
        }
    }

}
