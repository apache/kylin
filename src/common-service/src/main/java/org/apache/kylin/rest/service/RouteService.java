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

package org.apache.kylin.rest.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.resourcegroup.KylinInstance;
import org.apache.kylin.metadata.resourcegroup.RequestTypeEnum;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupMappingInfo;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class RouteService extends BasicService {
    private final ExecutorService asyncExecutors = new ThreadPoolExecutor(20, 20, 30, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(), new NamedThreadFactory("RouteScheduler"));

    public Boolean deleteAllFolderMultiTenantMode(HttpServletRequest request) {
        try {
            val rgManager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
            val resourceGroupServerNode = getResourceGroupServerNode(rgManager, RequestTypeEnum.QUERY);
            Map<Future<?>, Long> asyncFutures = Maps.newConcurrentMap();
            val startTime = System.currentTimeMillis();
            val routeServerCount = (int) resourceGroupServerNode.entrySet().stream()
                    .filter(entry -> CollectionUtils.isNotEmpty(entry.getValue())).count();
            val result = new CountDownLatch(routeServerCount);
            for (Map.Entry<String, List<KylinInstance>> entry : resourceGroupServerNode.entrySet()) {
                val kylinInstances = entry.getValue();
                if (CollectionUtils.isNotEmpty(kylinInstances)) {
                    val server = kylinInstances.get(RandomUtil.nextInt(kylinInstances.size()));
                    log.info("deleteAllFolder execute to groupId [{}] server [{}]", entry.getKey(),
                            server.getInstance());
                    executeAsyncTask(asyncFutures, () -> deleteAllFolder(server.getInstance(), request, result));
                }
            }
            cancelTimeoutAsyncTask(KylinConfig.getInstanceFromEnv(), asyncFutures, startTime,
                    "deleteAllFolderMultiTenantMode");
            return result.getCount() == 0;
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public Boolean deleteAllFolder(String instance, HttpServletRequest request, CountDownLatch result)
            throws Exception {
        String uri = StringUtils.stripEnd(request.getRequestURI(), "/");
        val url = "http://" + instance + uri + "/tenant_node";
        val response = generateTaskForRemoteHost(request, url);
        val data = (Boolean) response.getData();
        log.info("deleteAllFolder instance[{}] result : [{}]", instance, data);
        if (data) {
            result.countDown();
        }
        return data;
    }

    public void cancelTimeoutAsyncTask(KylinConfig kylinConfig, Map<Future<?>, Long> asyncFutures, long startTime,
            String message) throws InterruptedException {
        while (asyncFutures.size() > 0) {
            asyncFutures.forEach((asyncTask, start) -> {
                if (getRemainingTime(kylinConfig, start) <= 0) {
                    asyncTask.cancel(true);
                }
            });
            val doneTaskCount = asyncFutures.keySet().stream().filter(Future::isDone).count();
            if (doneTaskCount == asyncFutures.size()) {
                log.info("all running asyncTask[{}] is done", message);
                break;
            }
            if (getRemainingTime(kylinConfig, startTime) <= 0) {
                log.warn("cancel all running asyncTask[{}], DoneAsyncTask count: [{}], AllAsyncTask count : [{}]",
                        message, doneTaskCount, asyncFutures.size());
                asyncFutures.keySet().stream().filter(asyncTask -> !asyncTask.isDone())
                        .forEach(asyncTask -> asyncTask.cancel(true));
                break;
            }
            TimeUnit.SECONDS.sleep(10);
        }
    }

    private long getRemainingTime(KylinConfig kylinConfig, long startTime) {
        return kylinConfig.getKylinMultiTenantRouteTaskTimeOut() - (System.currentTimeMillis() - startTime);
    }

    public <T> void executeAsyncTask(Map<Future<?>, Long> asyncFutures, Callable<T> task) {
        val future = asyncExecutors.submit(task);
        asyncFutures.put(future, System.currentTimeMillis());
    }

    public Map<String, List<KylinInstance>> getResourceGroupServerNode(ResourceGroupManager rgManager,
            RequestTypeEnum requestType) {
        val servers = Maps.<String, List<KylinInstance>> newHashMap();
        val allResourceGroups = rgManager.getResourceGroup();
        val queryResourceGroups = allResourceGroups.getResourceGroupMappingInfoList().stream()
                .filter(resourceGroupMappingInfo -> resourceGroupMappingInfo.getRequestType().equals(requestType))
                .map(ResourceGroupMappingInfo::getResourceGroupId).collect(Collectors.toSet());
        allResourceGroups.getKylinInstances().stream()
                .filter(kylinInstance -> queryResourceGroups.contains(kylinInstance.getResourceGroupId()))
                .forEach(instance -> {
                    val instances = servers.getOrDefault(instance.getResourceGroupId(), Lists.newArrayList());
                    instances.add(instance);
                    servers.put(instance.getResourceGroupId(), instances);
                });
        return servers;
    }

    public boolean needRoute() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val rgManger = ResourceGroupManager.getInstance(kylinConfig);
        return kylinConfig.isKylinMultiTenantEnabled() && rgManger.isResourceGroupEnabled();
    }

    public void asyncRouteForMultiTenantMode(HttpServletRequest servletRequest, String url) {
        try {
            val rgManager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
            val resourceGroupServerNode = getResourceGroupServerNode(rgManager, RequestTypeEnum.BUILD);
            Map<Future<?>, Long> asyncFutures = Maps.newConcurrentMap();
            val startTime = System.currentTimeMillis();
            for (Map.Entry<String, List<KylinInstance>> entry : resourceGroupServerNode.entrySet()) {
                val kylinInstances = entry.getValue();
                if (CollectionUtils.isNotEmpty(kylinInstances)) {
                    val server = kylinInstances.get(RandomUtil.nextInt(kylinInstances.size()));
                    executeAsyncTask(asyncFutures, () -> {
                        val fullUrl = "http://" + server.getInstance() + url;
                        return generateTaskForRemoteHost(servletRequest, fullUrl);
                    });
                }
            }
            cancelTimeoutAsyncTask(KylinConfig.getInstanceFromEnv(), asyncFutures, startTime,
                    "cleanupStorageMultiTenantMode");
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    public String getProjectByJobIdUseInFilter(String jobId) {
        Preconditions.checkNotNull(jobId);
        val allProjects = getManager(NProjectManager.class).listAllProjects();
        for (ProjectInstance projectInstance : allProjects) {
            val executableManager = getManager(NExecutableManager.class, projectInstance.getName());
            val job = executableManager.getJob(jobId);
            if (Objects.nonNull(job)) {
                log.info("Job[{}] project is [{}]", jobId, job.getProject());
                return job.getProject();
            }
        }
        log.warn("Job[{}] can't get project, will route to _global node", jobId);
        return UnitOfWork.GLOBAL_UNIT;
    }

    public String getProjectByModelNameUseInFilter(String modelName) {
        Preconditions.checkNotNull(modelName);
        val allProjects = getManager(NProjectManager.class).listAllProjects();
        for (ProjectInstance project : allProjects) {
            NDataModel model = getMatchModels(modelName, project.getName());
            if (!Objects.isNull(model)) {
                log.info("[ModelName{}] project is [{}]", modelName, project.getName());
                return project.getName();
            }
        }
        log.warn("ModelName[{}] can't get project, will route to _global node", modelName);
        return UnitOfWork.GLOBAL_UNIT;
    }

    private NDataModel getMatchModels(String modelAlias, String projectName) {
        return getManager(NDataModelManager.class, projectName).listAllModels().stream()
                .filter(model -> model.getAlias().equalsIgnoreCase(modelAlias)).findFirst().orElse(null);
    }
}
