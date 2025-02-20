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

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.rest.service.RouteService.CACHE_GLUTEN_API;
import static org.apache.kylin.rest.service.RouteService.CACHE_GLUTEN_ASYNC_API;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.resourcegroup.KylinInstance;
import org.apache.kylin.metadata.resourcegroup.RequestTypeEnum;
import org.apache.kylin.metadata.resourcegroup.ResourceGroup;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.cluster.MockClusterManager;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GlutenCacheResponse;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;

import lombok.val;
import lombok.var;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.common.asyncprofiler.AsyncProfiler" })
@PrepareForTest({ HttpMethod.class, ResourceGroupManager.class, KylinConfig.class, KapConfig.class,
        AsyncQueryUtil.class })
public class RouteServiceTest {
    @InjectMocks
    private RouteService routeService = Mockito.spy(new RouteService());
    @Mock
    private RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    @Mock
    private ResourceGroupManager rgManager = Mockito.mock(ResourceGroupManager.class);
    @Mock
    private KylinConfig kylinConfig = Mockito.mock(KylinConfig.class);
    private ExecutorService executors;
    private ClusterManager clusterManager = new MockClusterManager();

    private void setField(String fieldName, Class fieldClass, Object fieldObject) {
        Field field = ReflectionUtils.findField(RouteService.class, fieldName, fieldClass);
        if (field == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Could not find field '%s' on %s", fieldName, "routeService"));
        }
        ReflectionUtils.makeAccessible(field);
        ReflectionUtils.setField(field, routeService, fieldObject);
    }

    @Before
    public void before() throws Exception {
        JobContextUtil.cleanUp();
        setField("restTemplate", RestTemplate.class, restTemplate);
        setField("clusterManager", ClusterManager.class, clusterManager);

        val restResult = JsonUtil.writeValueAsBytes(RestResponse.ok(true));
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<byte[]>> any())).thenReturn(resp);

        PowerMockito.mockStatic(HttpMethod.class, ResourceGroupManager.class, KylinConfig.class);
        PowerMockito.when(HttpMethod.valueOf(ArgumentMatchers.anyString())).thenReturn(HttpMethod.GET);
        PowerMockito.when(ResourceGroupManager.getInstance(ArgumentMatchers.any())).thenReturn(rgManager);
        PowerMockito.when(KylinConfig.getInstanceFromEnv()).thenReturn(kylinConfig);
        Mockito.when(kylinConfig.getKylinMultiTenantRouteTaskTimeOut()).thenReturn(30 * 60 * 1000L);
        Mockito.when(kylinConfig.getGlutenCacheRequestTimeout()).thenReturn(24 * 60 * 60 * 1000);

        val resourceGroupJson = "{\"create_time\":1669704879469,\"instances\":[{\"instance\":\"10.1.2.185:7878\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\"},{\"instance\":\"10.1.2.184:7878\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\"}],"
                + "\"mapping_info\":[{\"project\":\"184\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\",\"request_type\":\"BUILD\"},{\"project\":\"184\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\",\"request_type\":\"QUERY\"},{\"project\":\"185\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\",\"request_type\":\"BUILD\"},{\"project\":\"185\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\",\"request_type\":\"QUERY\"}],\"resource_groups\":[{\"id\":\"c444879a-b3b0-4946-aed1-018cbc946c4a\"},{\"id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\"},{\"id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\"}],\"uuid\":\"d5c316ed-b977-6efb-aea3-1735feb75d02\",\"last_modified\":1669952899667,\"version\":\"4.0.0.0\",\"resource_group_enabled\":true}\n";
        Mockito.when(rgManager.getResourceGroup())
                .thenReturn(JsonUtil.readValue(resourceGroupJson, new TypeReference<ResourceGroup>() {
                }));
        executors = new ThreadPoolExecutor(20, 20, 30, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
                new NamedThreadFactory("RouteScheduler"));
    }

    @After
    public void after() {
        if (Objects.nonNull(executors)) {
            ExecutorServiceUtil.shutdownGracefully(executors, 10);
        }
    }

    @Test
    public void deleteAllFolderMultiTenantMode() {
        val request = new MockHttpServletRequest();
        val result = routeService.deleteAllFolderMultiTenantMode(request);
        Assert.assertTrue(result);
    }

    @Test
    public void deleteAllFolderMultiTenantModeWithEmptyKylinInstances() {
        val servers = Maps.<String, List<KylinInstance>> newHashMap();
        servers.put("test", Lists.newArrayList());
        Mockito.doReturn(servers).when(routeService).getResourceGroupServerNode(
                ArgumentMatchers.any(ResourceGroupManager.class), ArgumentMatchers.any(RequestTypeEnum.class));
        val request = new MockHttpServletRequest();
        val result = routeService.deleteAllFolderMultiTenantMode(request);
        Assert.assertTrue(result);
    }

    @Test
    public void deleteAllFolder() throws Exception {
        val result = new CountDownLatch(1);
        val request = new MockHttpServletRequest();
        routeService.deleteAllFolder("127.0.0.1:8080", request, result);
        Assert.assertEquals(0, result.getCount());
    }

    @Test
    public void cancelTimeoutAsyncTask() throws InterruptedException {
        cancelTimeOutTask(10, 2);
        cancelTimeOutTask(0, 0);
    }

    private void cancelTimeOutTask(int second1, int second2) throws InterruptedException {
        Map<Future<?>, Long> asyncFutures = Maps.newConcurrentMap();
        val dateTime = new DateTime().plusMinutes(-30).plusSeconds(second1);
        for (int i = 0; i < 5; i++) {
            val futureTask = new FutureTask<String>(() -> null);
            val startTime = i % 2 == 0 ? dateTime.plusSeconds(-2) : dateTime.plusSeconds(second2);
            asyncFutures.put(futureTask, startTime.getMillis());
        }
        val kylinMultiTenantRouteTaskTimeOut = KylinConfig.getInstanceFromEnv().getKylinMultiTenantRouteTaskTimeOut();
        routeService.cancelTimeoutAsyncTask(kylinMultiTenantRouteTaskTimeOut, asyncFutures, dateTime.getMillis(),
                "test");
        asyncFutures.keySet().forEach(future -> Assert.assertTrue(future.isDone()));
    }

    @Test
    public void needRoute() {
        var needRoute = routeService.needRoute();
        Assert.assertFalse(needRoute);

        Mockito.when(kylinConfig.isKylinMultiTenantEnabled()).thenReturn(true);
        needRoute = routeService.needRoute();
        Assert.assertFalse(needRoute);

        Mockito.when(rgManager.isResourceGroupEnabled()).thenReturn(true);
        needRoute = routeService.needRoute();
        Assert.assertTrue(needRoute);
        Mockito.when(rgManager.isResourceGroupEnabled()).thenReturn(false);
    }

    @Test
    public void asyncRouteForMultiTenantMode() {
        val request = new MockHttpServletRequest();
        routeService.asyncRouteForMultiTenantMode(request, "url");
        Field field = ReflectionUtils.findField(RouteService.class, "asyncExecutors", ExecutorService.class);
        if (field == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Could not find field '%s' on %s", "asyncExecutors", "routeService"));
        }
        ReflectionUtils.makeAccessible(field);
        val asyncExecutors = ((ExecutorService) ReflectionUtils.getField(field, routeService));
        val threadPoolExecutor = (ThreadPoolExecutor) asyncExecutors;
        Assert.assertEquals(0, threadPoolExecutor.getActiveCount());
    }

    @Test
    public void asyncRouteForMultiTenantModeWithEmptyKylinInStances() {
        val servers = Maps.<String, List<KylinInstance>> newHashMap();
        servers.put("test", Lists.newArrayList());
        Mockito.doReturn(servers).when(routeService).getResourceGroupServerNode(
                ArgumentMatchers.any(ResourceGroupManager.class), ArgumentMatchers.any(RequestTypeEnum.class));
        val request = new MockHttpServletRequest();
        routeService.asyncRouteForMultiTenantMode(request, "url");
        Field field = ReflectionUtils.findField(RouteService.class, "asyncExecutors", ExecutorService.class);
        if (field == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Could not find field '%s' on %s", "asyncExecutors", "routeService"));
        }
        ReflectionUtils.makeAccessible(field);
        val asyncExecutors = ((ExecutorService) ReflectionUtils.getField(field, routeService));
        val threadPoolExecutor = (ThreadPoolExecutor) asyncExecutors;
        Assert.assertEquals(0, threadPoolExecutor.getActiveCount());
    }

    @Test
    public void getProjectByJobIdUseInFilter() {
        val projectManger = Mockito.mock(NProjectManager.class);
        Mockito.when(kylinConfig.getManager(NProjectManager.class)).thenReturn(projectManger);
        Mockito.when(projectManger.listAllProjects()).thenReturn(Lists.newArrayList());
        var project = routeService.getProjectByJobIdUseInFilter(RandomUtil.randomUUIDStr());
        Assert.assertEquals(UnitOfWork.GLOBAL_UNIT, project);

        val projectInstance = Mockito.mock(ProjectInstance.class);
        Mockito.when(projectInstance.getName()).thenReturn("default");
        Mockito.when(projectManger.listAllProjects()).thenReturn(Lists.newArrayList(projectInstance));
        val executableManager1 = Mockito.mock(ExecutableManager.class);
        Mockito.when(kylinConfig.getManager(projectInstance.getName(), ExecutableManager.class))
                .thenReturn(executableManager1);
        val job1 = RandomUtil.randomUUIDStr();
        Mockito.when(executableManager1.getJob(job1)).thenReturn(null);
        project = routeService.getProjectByJobIdUseInFilter(job1);
        Assert.assertEquals(UnitOfWork.GLOBAL_UNIT, project);

        val job2 = RandomUtil.randomUUIDStr();
        val executable = Mockito.mock(AbstractExecutable.class);
        Field field = ReflectionUtils.findField(AbstractExecutable.class, "project", String.class);
        if (field == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Could not find field '%s' on %s", "project", "AbstractExecutable"));
        }
        ReflectionUtils.makeAccessible(field);
        ReflectionUtils.setField(field, executable, projectInstance.getName());
        Mockito.when(executableManager1.getJob(job2)).thenReturn(executable);
        project = routeService.getProjectByJobIdUseInFilter(job2);
        Assert.assertEquals(projectInstance.getName(), project);
    }

    @Test
    public void getProjectByModelNameUseInFilter() {
        val projectManger = Mockito.mock(NProjectManager.class);
        Mockito.when(kylinConfig.getManager(NProjectManager.class)).thenReturn(projectManger);
        Mockito.when(projectManger.listAllProjects()).thenReturn(Lists.newArrayList());
        var project = routeService.getProjectByModelNameUseInFilter(RandomUtil.randomUUIDStr());
        Assert.assertEquals(UnitOfWork.GLOBAL_UNIT, project);

        val projectInstance = Mockito.mock(ProjectInstance.class);
        Mockito.when(projectInstance.getName()).thenReturn("default");
        Mockito.when(projectManger.listAllProjects()).thenReturn(Lists.newArrayList(projectInstance));

        val dataModelManager = Mockito.mock(NDataModelManager.class);
        Mockito.when(kylinConfig.getManager("default", NDataModelManager.class)).thenReturn(dataModelManager);
        val dataModel = Mockito.mock(NDataModel.class);
        Mockito.when(dataModel.getAlias()).thenReturn("test_model");
        Mockito.when(dataModel.getProject()).thenReturn("default");
        Mockito.when(dataModelManager.listAllModels()).thenReturn(Lists.newArrayList(dataModel));

        project = routeService.getProjectByModelNameUseInFilter("test_model");
        Assert.assertEquals("default", project);
    }

    @Test
    public void checkGlutenCacheLimits() {
        {
            Map<Future<?>, Long> asyncFutures = Maps.newConcurrentMap();
            for (int i = 0; i < 5; i++) {
                val response = i % 2 == 0 ? new EnvelopeResponse(KylinException.CODE_SUCCESS, "", "")
                        : new EnvelopeResponse(KylinException.CODE_UNDEFINED, "",
                                new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getQueryTooManyRunning())
                                        .getLocalizedMessage());

                val futureTask = new FutureTask<>(() -> {
                }, response);
                executors.submit(futureTask);
                asyncFutures.put(futureTask, System.currentTimeMillis());
            }
            var result = routeService.checkGlutenCacheLimits(asyncFutures);
            Assert.assertTrue(result);
        }

        {
            Map<Future<?>, Long> asyncFutures = Maps.newConcurrentMap();
            for (int i = 0; i < 5; i++) {
                val response = new EnvelopeResponse(KylinException.CODE_SUCCESS, "", "");
                val futureTask = new FutureTask<>(() -> {
                }, response);
                executors.submit(futureTask);
                asyncFutures.put(futureTask, System.currentTimeMillis());
            }
            val result = routeService.checkGlutenCacheLimits(asyncFutures);
            Assert.assertFalse(result);
        }

        {
            Map<Future<?>, Long> asyncFutures = Maps.newConcurrentMap();
            for (int i = 0; i < 5; i++) {
                val futureTask = new FutureTask<>(() -> {
                }, "test");
                executors.submit(futureTask);
                asyncFutures.put(futureTask, System.currentTimeMillis());
            }
            val result = routeService.checkGlutenCacheLimits(asyncFutures);
            Assert.assertFalse(result);
        }
    }

    private void testRouteGlutenCacheInner(String url, boolean checkLimits) throws Exception {
        val request = new MockHttpServletRequest();
        List<String> cacheCommands = Lists.newArrayList("test command");
        val result = routeService.routeGlutenCacheInner(cacheCommands, request, url, checkLimits);
        Assert.assertTrue(result);
    }

    @Test
    public void routeGlutenCache() throws Exception {
        val glutenResponse = new GlutenCacheResponse(true, Lists.newArrayList(), "");
        val restResult = JsonUtil.writeValueAsBytes(RestResponse.ok(glutenResponse));
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<byte[]>> any())).thenReturn(resp);
        testRouteGlutenCacheInner(CACHE_GLUTEN_API, true);
        testRouteGlutenCacheInner(CACHE_GLUTEN_API, false);

        val request = new MockHttpServletRequest();
        List<String> cacheCommands = Lists.newArrayList();
        val result = routeService.routeGlutenCache(cacheCommands, request);
        Assert.assertTrue(result);
    }

    @Test
    public void routeGlutenCacheAsync() throws Exception {
        val restResult = JsonUtil.writeValueAsBytes(RestResponse.ok(""));
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<byte[]>> any())).thenReturn(resp);
        testRouteGlutenCacheInner(CACHE_GLUTEN_ASYNC_API, true);
        testRouteGlutenCacheInner(CACHE_GLUTEN_ASYNC_API, false);

        val request = new MockHttpServletRequest();
        List<String> cacheCommands = Lists.newArrayList();
        routeService.routeGlutenCacheAsync(cacheCommands, request);
    }
}
