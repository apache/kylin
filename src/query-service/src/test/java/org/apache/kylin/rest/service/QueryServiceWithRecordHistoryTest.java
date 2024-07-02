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

import static org.apache.kylin.common.debug.BackdoorToggles.CONNECTION_CREATING_TIME;
import static org.apache.kylin.common.debug.BackdoorToggles.STATEMENT_TO_REQUEST_TIME;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.QueryRoutingEngine;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.cluster.DefaultClusterManager;
import org.apache.kylin.rest.config.AppConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

/**
 * @author xduo
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class, SparkSession.class, QueryService.class,
        NIndexPlanManager.class, })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
public class QueryServiceWithRecordHistoryTest extends NLocalFileMetadataTestCase {

    private final QueryCacheManager queryCacheManager = new QueryCacheManager();

    private final ClusterManager clusterManager = new DefaultClusterManager(8080);

    @Mock
    private QueryService queryService;

    @InjectMocks
    private final AppConfig appConfig = Mockito.spy(new AppConfig());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    protected UserAclService userAclService = Mockito.spy(UserAclService.class);

    @Mock
    protected AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    protected UserService userService = Mockito.spy(KylinUserService.class);

    @Mock
    protected AclService aclService = Mockito.spy(AclService.class);

    @Mock
    protected AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @Before
    public void setup() throws Exception {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);
        overwriteSystemProp("kylin.query.transaction-enable", "true");
        overwriteSystemProp("kylin.query.cache-threshold-duration", String.valueOf(-1));
        overwriteSystemProp("HADOOP_USER_NAME", "root");

        createTestMetadata();

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        queryService = Mockito.spy(new QueryService());
        queryService.queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);
        Mockito.when(SpringContext.getBean(CacheSignatureQuerySupporter.class)).thenReturn(queryService);
        Mockito.when(appConfig.getPort()).thenReturn(7070);
        Mockito.when(SpringContext.getBean("queryService")).thenReturn(queryService);
        ReflectionTestUtils.setField(queryService, "aclEvaluate", Mockito.mock(AclEvaluate.class));
        ReflectionTestUtils.setField(queryService, "queryCacheManager", queryCacheManager);
        ReflectionTestUtils.setField(queryService, "clusterManager", clusterManager);
        ReflectionTestUtils.setField(queryService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(queryService, "accessService", accessService);
        ReflectionTestUtils.setField(queryService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(accessService, "userService", userService);
        ReflectionTestUtils.setField(accessService, "aclService", aclService);
        ReflectionTestUtils.setField(accessService, "userAclService", userAclService);
        ReflectionTestUtils.setField(userAclService, "userService", userService);
        ReflectionTestUtils.setField(aclTCRService, "accessService", accessService);
        ReflectionTestUtils.setField(aclTCRService, "userService", userService);
        ReflectionTestUtils.setField(queryService, "appConfig", appConfig);
        ReflectionTestUtils.setField(userService, "userAclService", userAclService);

        userService.createUser(new ManagedUser("ADMIN", "KYLIN", false,
                Collections.singletonList(new UserGrantedAuthority("ROLE_ADMIN"))));
        queryCacheManager.init();
        Mockito.doNothing().when(userAclService).updateUserAclPermission(Mockito.any(UserDetails.class),
                Mockito.any(Permission.class));
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    private void stubQueryConnection(final String sql, final String project) throws Exception {
        final QueryResult queryResult = Mockito.mock(QueryResult.class);
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        queryService.queryRoutingEngine = Mockito.mock(QueryRoutingEngine.class);
        Mockito.when(queryExec.executeQuery(sql)).thenReturn(queryResult);
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project);
        Mockito.when(queryService.newQueryExec(project)).thenReturn(queryExec);
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project, null);
        Mockito.when(queryService.newQueryExec(project, null)).thenReturn(queryExec);
        Mockito.when(queryService.queryRoutingEngine.queryWithSqlMassage(Mockito.any())).thenReturn(new QueryResult());
    }

    private void mockOlapContext() throws Exception {
        val modelManager = Mockito.spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default"));

        Mockito.doReturn(modelManager).when(queryService).getManager(NDataModelManager.class, "default");
        // mock agg index realization
        OlapContext aggMock = new OlapContext(1);
        NDataModel mockModel1 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel1.getUuid()).thenReturn("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Mockito.when(mockModel1.getAlias()).thenReturn("mock_model_alias1");
        Mockito.doReturn(mockModel1).when(modelManager).getDataModelDesc("mock_model1");
        IRealization mockRealization1 = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization1.getModel()).thenReturn(mockModel1);
        aggMock.setRealization(mockRealization1);
        IndexEntity mockIndexEntity1 = new IndexEntity();
        mockIndexEntity1.setId(0L);
        LayoutEntity mockLayout1 = new LayoutEntity();
        mockLayout1.setId(1L);
        mockLayout1.setIndex(mockIndexEntity1);
        NLayoutCandidate layoutCandidate1 = new NLayoutCandidate(mockLayout1);
        layoutCandidate1.setPrunedSegments(Lists.newArrayList(new NDataSegment()));
        aggMock.getStorageContext().setBatchCandidate(layoutCandidate1);
        ContextUtil.registerContext(aggMock);

        // mock table index realization
        OlapContext tableMock = new OlapContext(2);
        NDataModel mockModel2 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel2.getUuid()).thenReturn("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Mockito.when(mockModel2.getAlias()).thenReturn("mock_model_alias2");
        Mockito.doReturn(mockModel2).when(modelManager).getDataModelDesc("mock_model2");
        IRealization mockRealization2 = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization2.getModel()).thenReturn(mockModel2);
        tableMock.setRealization(mockRealization2);
        IndexEntity mockIndexEntity2 = new IndexEntity();
        mockIndexEntity2.setId(IndexEntity.TABLE_INDEX_START_ID);
        LayoutEntity mockLayout2 = new LayoutEntity();
        mockLayout2.setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        mockLayout2.setIndex(mockIndexEntity2);
        NLayoutCandidate layoutCandidate2 = new NLayoutCandidate(mockLayout2);
        layoutCandidate2.setPrunedSegments(Lists.newArrayList(new NDataSegment()));
        tableMock.getStorageContext().setBatchCandidate(layoutCandidate2);
        ContextUtil.registerContext(tableMock);
        mockQueryWithSqlMassage();
    }

    private void mockQueryWithSqlMassage() throws Exception {
        Mockito.doAnswer(invocation -> {
                    QueryContext.current().setQueryRealizations(ContextUtil.getNativeRealizations());
                    return new QueryResult();
                }).when(queryService.queryRoutingEngine)
                .queryWithSqlMassage(Mockito.any());
    }

    @Test
    public void testQueryHistory() throws Exception {
        testQueryHistory(true);
        testQueryHistory(false);
        testQueryHistory(true);
        testQueryHistory(false);
    }

    private void testQueryHistory(boolean hasBackdoorToggles) throws Exception {
        QueryHistoryScheduler queryHistoryScheduler = QueryHistoryScheduler.getInstance();
        Assert.assertEquals(0, queryHistoryScheduler.queryMetricsQueue.size());

        final String sql = "select * from success_table";
        final String project = "default";
        stubQueryConnection(sql, project);
        mockOlapContext();

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        if (hasBackdoorToggles) {
            Map<String, String> backdoorToggles = ImmutableBiMap.of(CONNECTION_CREATING_TIME, "123",
                    STATEMENT_TO_REQUEST_TIME, "321");
            request.setBackdoorToggles(backdoorToggles);
        }

        // case of not hitting cache
        String expectedQueryID = QueryContext.current().getQueryId();
        //Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        Mockito.when(SpringContext.getBean(CacheSignatureQuerySupporter.class)).thenReturn(queryService);
        //Mockito.when(queryService.createAclSignature(project)).thenReturn("root");
        final SQLResponse firstSuccess = queryService.queryWithCache(request);
        Assert.assertEquals(expectedQueryID, firstSuccess.getQueryId());
        Assert.assertEquals(2, firstSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, firstSuccess.getNativeRealizations().get(0).getType());
        Assert.assertEquals(QueryMetricsContext.TABLE_INDEX, firstSuccess.getNativeRealizations().get(1).getType());
        Assert.assertEquals(Lists.newArrayList("mock_model_alias1", "mock_model_alias2"),
                firstSuccess.getNativeRealizations().stream().map(NativeQueryRealization::getModelAlias)
                        .collect(Collectors.toList()));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(1, queryHistoryScheduler.queryMetricsQueue.size()));

        List<QueryMetrics> queryMetricsList = Lists.newArrayList();
        queryHistoryScheduler.queryMetricsQueue.drainTo(queryMetricsList);
        QueryMetrics queryMetrics = queryMetricsList.get(0);
        List<QueryHistoryInfo.QueryTraceSpan> traces = queryMetrics.getQueryHistoryInfo().getTraces();
        if (hasBackdoorToggles) {
            Assert.assertTrue(traces.stream()
                    .anyMatch(span -> CONNECTION_CREATING_TIME.equals(span.getName()) && span.getDuration() == 123));
            Assert.assertTrue(traces.stream()
                    .anyMatch(span -> STATEMENT_TO_REQUEST_TIME.equals(span.getName()) && span.getDuration() == 321));
        } else {
            Assert.assertFalse(traces.stream().anyMatch(span -> CONNECTION_CREATING_TIME.equals(span.getName())
                    || STATEMENT_TO_REQUEST_TIME.equals(span.getName())));
        }
        Assert.assertEquals(0, queryHistoryScheduler.queryMetricsQueue.size());
        queryCacheManager.clearQueryCache(request);
    }
}
