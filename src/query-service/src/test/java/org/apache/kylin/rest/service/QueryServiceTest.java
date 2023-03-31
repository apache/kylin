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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kylin.common.QueryContext.PUSHDOWN_HIVE;
import static org.apache.kylin.common.QueryTrace.EXECUTION;
import static org.apache.kylin.common.QueryTrace.SPARK_JOB_EXECUTION;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.rest.metrics.QueryMetricsContextTest.getInfluxdbFields;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.acls.domain.BasePermission.ADMINISTRATION;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.ForceToTieredStorage;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.exception.ResourceLimitExceededException;
import org.apache.kylin.common.hystrix.NCircuitBreaker;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.ColumnMetaWithType;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.query.blacklist.SQLBlacklistItem;
import org.apache.kylin.query.blacklist.SQLBlacklistManager;
import org.apache.kylin.query.engine.PrepareSqlStateParam;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.QueryRoutingEngine;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.DateNumberFilterTransformer;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.RawSqlParser;
import org.apache.kylin.query.util.SlowQueryDetector;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.cluster.DefaultClusterManager;
import org.apache.kylin.rest.config.AppConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.QueryCacheSignatureUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.source.adhocquery.PushdownResult;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
import org.springframework.security.acls.model.Sid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

/**
 * @author xduo
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class, SparkSession.class, QueryService.class })
@PowerMockIgnore({ "javax.management.*" })
public class QueryServiceTest extends NLocalFileMetadataTestCase {

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

    private void stubQueryConnectionException() throws Exception {
        Mockito.when(queryService.queryRoutingEngine.queryWithSqlMassage(Mockito.any()))
                .thenThrow(new RuntimeException(new ResourceLimitExceededException("")));
    }

    @Test
    public void testQueryPushDownForced() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToPushDown(true);

        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                queryExec.getDefaultSchemaName(), true);
        String correctedSql = QueryUtil.massageSql(queryParams);

        Mockito.when(queryExec.executeQuery(correctedSql))
                .thenThrow(new RuntimeException("shouldn't execute executeQuery"));
        Mockito.doThrow(new RuntimeException("shouldn't execute searchCache")).when(queryService)
                .searchCache(Mockito.any(), Mockito.any());

        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project);
        Mockito.when(queryService.newQueryExec(project)).thenReturn(queryExec);
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project, null);
        Mockito.when(queryService.newQueryExec(project, null)).thenReturn(queryExec);

        Mockito.doAnswer(invocation -> PushdownResult.emptyResult()).when(queryService.queryRoutingEngine)
                .tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        final SQLResponse response = queryService.queryWithCache(sqlRequest);

        Assert.assertFalse(response.isStorageCacheUsed());
        Assert.assertTrue(response.isQueryPushDown());
    }

    @Test
    public void testQueryPushDownWhenForceToTieredStorageEqualsOne() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToPushDown(false);
        sqlRequest.setForcedToTieredStorage(1);

        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                queryExec.getDefaultSchemaName(), true);
        QueryUtil.massageSql(queryParams);

        overwriteSystemProp("kylin.query.pushdown-enabled", "false");
        Mockito.doThrow(new SQLException(new SQLException(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE)))
                .when(queryService.queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        final SQLResponse response = queryService.queryWithCache(sqlRequest);
        Assert.assertEquals(MsgPicker.getMsg().getDisablePushDownPrompt(), response.getExceptionMessage());
    }

    @Test
    public void testQueryPushDownWhenNormalDisable() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToTieredStorage(1);

        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                queryExec.getDefaultSchemaName(), true);
        String correctedSql = QueryUtil.massageSql(queryParams);

        overwriteSystemProp("kylin.query.pushdown-enabled", "false");
        Mockito.doThrow(new SQLException(new SQLException("No model found for OLAPContex")))
                .when(queryService.queryRoutingEngine).execute(Mockito.anyString(), Mockito.any());

        final SQLResponse response = queryService.queryWithCache(sqlRequest);
        Assert.assertNotEquals(MsgPicker.getMsg().getDisablePushDownPrompt(), response.getExceptionMessage());
    }

    @Test
    public void testQueryIndexForced() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToIndex(true);

        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                queryExec.getDefaultSchemaName(), true);
        String correctedSql = QueryUtil.massageSql(queryParams);

        Mockito.when(queryExec.executeQuery(correctedSql))
                .thenThrow(new RuntimeException("shouldnt execute queryexec"));
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project);
        Mockito.when(queryService.newQueryExec(project)).thenReturn(queryExec);
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project, null);
        Mockito.when(queryService.newQueryExec(project, null)).thenReturn(queryExec);

        Mockito.doAnswer(invocation -> PushdownResult.emptyResult()).when(queryService.queryRoutingEngine)
                .tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        final SQLResponse response = queryService.queryWithCache(sqlRequest);
        Assert.assertFalse(response.isQueryPushDown());
    }

    @Test
    public void testQueryPushDownErrorMessage() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";

        Mockito.doAnswer(invocation -> {
            QueryContext.current().setPushdownEngine(PUSHDOWN_HIVE);
            throw new SQLException("push down error");
        }).when(queryService.queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(),
                Mockito.anyBoolean());

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        final SQLResponse response = queryService.queryWithCache(request);
        Assert.assertTrue(response.isException());
        Assert.assertTrue(StringUtils.contains(response.getExceptionMessage(), "[HIVE Exception] push down error"));
    }

    @Test
    public void testQueryStackOverflowError() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";

        Mockito.doAnswer(invocation -> {
            throw new StackOverflowError();
        }).when(queryService.queryRoutingEngine).queryWithSqlMassage(Mockito.any());

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        final SQLResponse response = queryService.queryWithCache(request);
        Assert.assertTrue(response.isException());
        Assert.assertTrue(StringUtils.contains(response.getExceptionMessage(), "java.lang.StackOverflowError"));
    }

    @Test
    public void testQueryWithCacheFailedForProjectNotExist() {
        final String sql = "select * from success_table";
        final String notExistProject = "default0";
        final SQLRequest request = new SQLRequest();
        request.setProject(notExistProject);
        request.setSql(sql);
        try {
            queryService.queryWithCache(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(PROJECT_NOT_EXIST.getMsg("default0"), e.getMessage());
        }
    }

    @Test
    public void testQueryWithCacheFailedForSqlNotExist() {
        final String sql = "";
        final String notExistProject = "default";
        final SQLRequest request = new SQLRequest();
        request.setProject(notExistProject);
        request.setSql(sql);
        try {
            queryService.queryWithCache(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("SQL can’t be empty. Please check and try again.", e.getMessage());
        }
    }

    @Test
    public void testQueryWithCache() throws Exception {
        final String sql = "select * from success_table";
        final String project = "default";
        stubQueryConnection(sql, project);
        mockOLAPContext();

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        // case of not hitting cache
        String expectedQueryID = QueryContext.current().getQueryId();
        //Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        Mockito.when(SpringContext.getBean(CacheSignatureQuerySupporter.class)).thenReturn(queryService);
        //Mockito.when(queryService.createAclSignature(project)).thenReturn("root");
        final SQLResponse firstSuccess = queryService.queryWithCache(request);
        Assert.assertEquals(expectedQueryID, firstSuccess.getQueryId());
        Assert.assertEquals(2, firstSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, firstSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals(QueryMetricsContext.TABLE_INDEX,
                firstSuccess.getNativeRealizations().get(1).getIndexType());
        Assert.assertEquals(Lists.newArrayList("mock_model_alias1", "mock_model_alias2"),
                firstSuccess.getNativeRealizations().stream().map(NativeQueryRealization::getModelAlias)
                        .collect(Collectors.toList()));
        // assert log info
        String log = queryService.logQuery(request, firstSuccess);
        Assert.assertTrue(log.contains("mock_model_alias1"));
        Assert.assertTrue(log.contains("mock_model_alias2"));

        // case of hitting cache
        expectedQueryID = QueryContext.current().getQueryId();
        final SQLResponse secondSuccess = queryService.queryWithCache(request);
        Assert.assertTrue(secondSuccess.isStorageCacheUsed());
        Assert.assertEquals(expectedQueryID, secondSuccess.getQueryId());
        Assert.assertEquals(2, secondSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, secondSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals(QueryMetricsContext.TABLE_INDEX,
                secondSuccess.getNativeRealizations().get(1).getIndexType());
        // mock realization, return true model name by model id
        Assert.assertEquals("nmodel_basic", secondSuccess.getNativeRealizations().get(0).getModelAlias());
        // assert log info
        log = queryService.logQuery(request, secondSuccess);
        Assert.assertTrue(log.contains("nmodel_basic"));
        Assert.assertTrue(log.contains("nmodel_basic_inner"));
    }

    private void mockOLAPContextForEmptyLayout() throws Exception {
        val modelManager = Mockito.spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default"));

        Mockito.doReturn(modelManager).when(queryService).getManager(NDataModelManager.class, "default");
        // mock empty index realization
        OLAPContext mock = new OLAPContext(1);
        NDataModel mockModel1 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel1.getUuid()).thenReturn("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Mockito.when(mockModel1.getAlias()).thenReturn("mock_model_alias1");
        Mockito.doReturn(mockModel1).when(modelManager).getDataModelDesc("mock_model1");
        IRealization mockRealization1 = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization1.getModel()).thenReturn(mockModel1);
        mock.realization = mockRealization1;
        mock.storageContext.setEmptyLayout(true);
        mock.storageContext.setCandidate(NLayoutCandidate.EMPTY);
        mock.storageContext.setLayoutId(null);
        mock.storageContext.setPrunedSegments(Lists.newArrayList());
        OLAPContext.registerContext(mock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
        mockQueryWithSqlMassage();
    }

    private void mockOLAPContext() throws Exception {
        val modelManager = Mockito.spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default"));

        Mockito.doReturn(modelManager).when(queryService).getManager(NDataModelManager.class, "default");
        // mock agg index realization
        OLAPContext aggMock = new OLAPContext(1);
        NDataModel mockModel1 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel1.getUuid()).thenReturn("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Mockito.when(mockModel1.getAlias()).thenReturn("mock_model_alias1");
        Mockito.doReturn(mockModel1).when(modelManager).getDataModelDesc("mock_model1");
        IRealization mockRealization1 = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization1.getModel()).thenReturn(mockModel1);
        aggMock.realization = mockRealization1;
        IndexEntity mockIndexEntity1 = new IndexEntity();
        mockIndexEntity1.setId(1);
        LayoutEntity mockLayout1 = new LayoutEntity();
        mockLayout1.setIndex(mockIndexEntity1);
        aggMock.storageContext.setCandidate(new NLayoutCandidate(mockLayout1));
        aggMock.storageContext.setLayoutId(1L);
        aggMock.storageContext.setPrunedSegments(Lists.newArrayList(new NDataSegment()));
        OLAPContext.registerContext(aggMock);

        // mock table index realization
        OLAPContext tableMock = new OLAPContext(2);
        NDataModel mockModel2 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel2.getUuid()).thenReturn("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Mockito.when(mockModel2.getAlias()).thenReturn("mock_model_alias2");
        Mockito.doReturn(mockModel2).when(modelManager).getDataModelDesc("mock_model2");
        IRealization mockRealization2 = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization2.getModel()).thenReturn(mockModel2);
        tableMock.realization = mockRealization2;
        IndexEntity mockIndexEntity2 = new IndexEntity();
        mockIndexEntity2.setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        LayoutEntity mockLayout2 = new LayoutEntity();
        mockLayout2.setIndex(mockIndexEntity2);
        tableMock.storageContext.setCandidate(new NLayoutCandidate(mockLayout2));
        tableMock.storageContext.setLayoutId(1L);
        tableMock.storageContext.setPrunedSegments(Lists.newArrayList(new NDataSegment()));
        OLAPContext.registerContext(tableMock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
        mockQueryWithSqlMassage();
    }

    private void mockOLAPContextWithHybrid() throws Exception {
        val modelManager = Mockito
                .spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test"));

        Mockito.doReturn(modelManager).when(queryService).getManager(NDataModelManager.class, "streaming_test");
        // mock agg index realization
        OLAPContext aggMock = new OLAPContext(1);
        NDataModel mockModel1 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel1.getUuid()).thenReturn("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        Mockito.when(mockModel1.getAlias()).thenReturn("model_streaming");
        Mockito.doReturn(mockModel1).when(modelManager).getDataModelDesc("4965c827-fbb4-4ea1-a744-3f341a3b030d");

        IRealization batchRealization = Mockito.mock(IRealization.class);
        Mockito.when(batchRealization.getUuid()).thenReturn("cd2b9a23-699c-4699-b0dd-38c9412b3dfd");

        HybridRealization hybridRealization = Mockito.mock(HybridRealization.class);
        Mockito.when(hybridRealization.getModel()).thenReturn(mockModel1);
        Mockito.when(hybridRealization.getBatchRealization()).thenReturn(batchRealization);

        aggMock.realization = hybridRealization;
        IndexEntity mockIndexEntity1 = new IndexEntity();
        mockIndexEntity1.setId(1);
        LayoutEntity mockLayout1 = new LayoutEntity();
        mockLayout1.setIndex(mockIndexEntity1);
        aggMock.storageContext.setCandidate(new NLayoutCandidate(mockLayout1));
        aggMock.storageContext.setLayoutId(20001L);
        aggMock.storageContext.setStreamingLayoutId(10001L);
        aggMock.storageContext.setPrunedSegments(Lists.newArrayList(new NDataSegment()));
        OLAPContext.registerContext(aggMock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
        mockQueryWithSqlMassage();
    }

    private void mockOLAPContextWithBatchPart() throws Exception {
        val modelManager = Mockito
                .spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test"));

        Mockito.doReturn(modelManager).when(queryService).getManager(NDataModelManager.class, "streaming_test");
        // mock agg index realization
        OLAPContext aggMock = new OLAPContext(1);
        NDataModel mockModel1 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel1.getUuid()).thenReturn("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        Mockito.when(mockModel1.getAlias()).thenReturn("model_streaming");
        Mockito.doReturn(mockModel1).when(modelManager).getDataModelDesc("4965c827-fbb4-4ea1-a744-3f341a3b030d");

        IRealization batchRealization = Mockito.mock(IRealization.class);
        Mockito.when(batchRealization.getUuid()).thenReturn("cd2b9a23-699c-4699-b0dd-38c9412b3dfd");

        HybridRealization hybridRealization = Mockito.mock(HybridRealization.class);
        Mockito.when(hybridRealization.getModel()).thenReturn(mockModel1);
        Mockito.when(hybridRealization.getBatchRealization()).thenReturn(batchRealization);

        aggMock.realization = hybridRealization;
        IndexEntity mockIndexEntity1 = new IndexEntity();
        mockIndexEntity1.setId(1);
        LayoutEntity mockLayout1 = new LayoutEntity();
        mockLayout1.setIndex(mockIndexEntity1);
        aggMock.storageContext.setCandidate(new NLayoutCandidate(mockLayout1));
        aggMock.storageContext.setLayoutId(20001L);
        aggMock.storageContext.setPrunedSegments(Lists.newArrayList(new NDataSegment()));
        OLAPContext.registerContext(aggMock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
        mockQueryWithSqlMassage();
    }

    private void mockOLAPContextWithStreaming() throws Exception {
        val modelManager = Mockito.spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "demo"));

        Mockito.doReturn(modelManager).when(queryService).getManager(NDataModelManager.class, "demo");
        // mock agg index realization
        OLAPContext aggMock = new OLAPContext(1);
        NDataModel mockModel1 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel1.getUuid()).thenReturn("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        Mockito.when(mockModel1.getAlias()).thenReturn("model_streaming");
        Mockito.doReturn(mockModel1).when(modelManager).getDataModelDesc("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        IRealization realization = Mockito.mock(IRealization.class);
        Mockito.when(realization.getModel()).thenReturn(mockModel1);
        aggMock.realization = realization;
        IndexEntity mockIndexEntity1 = new IndexEntity();
        mockIndexEntity1.setId(1);
        LayoutEntity mockLayout1 = new LayoutEntity();
        mockLayout1.setIndex(mockIndexEntity1);
        aggMock.storageContext.setStreamingCandidate(new NLayoutCandidate(mockLayout1));
        aggMock.storageContext.setStreamingLayoutId(10001L);
        aggMock.storageContext.setPrunedStreamingSegments(Lists.newArrayList(new NDataSegment()));
        OLAPContext.registerContext(aggMock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
        mockQueryWithSqlMassage();
    }

    private void mockQueryWithSqlMassage() throws Exception {
        Mockito.doAnswer(invocation -> new QueryResult()).when(queryService.queryRoutingEngine)
                .queryWithSqlMassage(Mockito.any());
    }

    private void mockOLAPContextWithOneModelInfo(String modelId, String modelAlias, long layoutId) throws Exception {
        final OLAPContext mock = new OLAPContext(1);

        final NDataModel mockModel = Mockito.spy(new NDataModel());
        Mockito.when(mockModel.getUuid()).thenReturn(modelId);
        Mockito.when(mockModel.getAlias()).thenReturn(modelAlias);
        final IRealization mockRealization = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization.getModel()).thenReturn(mockModel);
        mock.realization = mockRealization;

        final IndexEntity mockIndexEntity = new IndexEntity();
        mockIndexEntity.setId(layoutId);
        final LayoutEntity mockLayout = new LayoutEntity();
        mockLayout.setIndex(mockIndexEntity);
        mock.storageContext.setCandidate(new NLayoutCandidate(mockLayout));
        mock.storageContext.setLayoutId(layoutId);
        mock.storageContext.setPrunedSegments(Lists.newArrayList(new NDataSegment()));

        OLAPContext.registerContext(mock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();

        mockQueryWithSqlMassage();
    }

    @Test
    public void testQueryWithTimeOutException() throws Exception {
        final String sql = "select * from exception_table";
        final String project = "newten";

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        Mockito.doThrow(new RuntimeException(new KylinTimeoutException("calcite timeout exception"))).when(queryService)
                .query(request);

        final SQLResponse sqlResponse = queryService.queryWithCache(request);
        Assert.assertTrue(sqlResponse.isException());
        String log = queryService.logQuery(request, sqlResponse);
        Assert.assertTrue(log.contains("Is Timeout: true"));
    }

    @Test
    public void testQueryWithCacheException() throws Throwable {
        final String sql = "select * from exception_table";
        final String project = "default";
        stubQueryConnection(sql, project);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        stubQueryConnectionException();
        try {
            final String expectedQueryID = QueryContext.current().getQueryId();
            final SQLResponse response = queryService.queryWithCache(request);
            Assert.assertEquals(false, response.isHitExceptionCache());
            Assert.assertEquals(true, response.isException());
            Assert.assertEquals(expectedQueryID, response.getQueryId());
        } catch (InternalErrorException ex) {
            // ignore
        }

        try {
            final String expectedQueryID = QueryContext.current().getQueryId();
            final SQLResponse response = queryService.queryWithCache(request);
            Assert.assertEquals(true, response.isHitExceptionCache());
            Assert.assertEquals(true, response.isException());
            Assert.assertEquals(expectedQueryID, response.getQueryId());
        } catch (InternalErrorException ex) {
            // ignore
        }
    }

    @Test
    public void testCreateTableToWith() throws IOException {
        String create_table1 = " create table tableId as select * from some_table1;";
        String create_table2 = "CREATE TABLE tableId2 AS select * FROM some_table2;";
        String select_table = "select * from tableId join tableId2 on tableId.a = tableId2.b;";

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.convert-create-table-to-with", "true");
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {

            SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(create_table1);
            queryService.queryWithCache(request);

            request.setSql(create_table2);
            queryService.queryWithCache(request);

            request.setSql(select_table);
            SQLResponse response = queryService.queryWithCache(request);

            Assert.assertEquals("From line 1, column 32 to line 1, column 42: Object 'SOME_TABLE1' not found\n"
                    + "while executing SQL: \"WITH tableId as (select * from some_table1), tableId2 AS (select * FROM some_table2) select * from tableId join tableId2 on tableId.a = tableId2.b\"",
                    response.getExceptionMessage());
        }
    }

    @Test
    public void testExposedColumnsProjectConfig() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        // expose computed column
        {
            projectManager.updateProject("default", copyForWrite -> copyForWrite.getOverrideKylinProps()
                    .put("kylin.query.metadata.expose-computed-column", "true"));
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);

            List<ColumnMeta> factColumns;
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertTrue(getColumnNames(factColumns).containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT",
                    "LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }

        // hide computed column
        {
            projectManager.updateProject("default", copyForWrite -> copyForWrite.getOverrideKylinProps()
                    .put("kylin.query.metadata.expose-computed-column", "false"));
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);

            List<ColumnMeta> factColumns;
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(columnDescs.length, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns).containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT",
                    "LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }
    }

    @Test
    public void testGetMetadataAddType() throws Exception {
        List<TableMetaWithType> tableMetasAddType = queryService.getMetadataAddType("default", null);
        List<TableMeta> tableMetas = queryService.getMetadata("default", null);
        List<TableMeta> tablesV2 = Lists.newLinkedList();
        for (TableMetaWithType t : tableMetasAddType) {
            TableMeta tableMeta = new TableMeta(t.getTABLE_CAT(), t.getTABLE_SCHEM(), t.getTABLE_NAME(),
                    t.getTABLE_TYPE(), t.getREMARKS(), t.getTYPE_CAT(), t.getTYPE_SCHEM(), t.getTYPE_NAME(),
                    t.getSELF_REFERENCING_COL_NAME(), t.getREF_GENERATION());
            tableMeta.setColumns(t.getColumns().stream()
                    .map(c -> new ColumnMeta(c.getTABLE_CAT(), c.getTABLE_SCHEM(), c.getTABLE_NAME(),
                            c.getCOLUMN_NAME(), c.getDATA_TYPE(), c.getTYPE_NAME(), c.getCOLUMN_SIZE(),
                            c.getBUFFER_LENGTH(), c.getDECIMAL_DIGITS(), c.getNUM_PREC_RADIX(), c.getNULLABLE(),
                            c.getREMARKS(), c.getCOLUMN_DEF(), c.getSQL_DATA_TYPE(), c.getSQL_DATETIME_SUB(),
                            c.getCHAR_OCTET_LENGTH(), c.getORDINAL_POSITION(), c.getIS_NULLABLE(), c.getSCOPE_CATLOG(),
                            c.getSCOPE_SCHEMA(), c.getSCOPE_TABLE(), c.getSOURCE_DATA_TYPE(), c.getIS_AUTOINCREMENT()))
                    .collect(Collectors.toList()));
            tablesV2.add(tableMeta);
        }
        Assert.assertEquals(JsonUtil.writeValueAsString(tablesV2), JsonUtil.writeValueAsString(tableMetas));

        tableMetasAddType = queryService.getMetadataAddType("default", "test_bank");
        tableMetas = queryService.getMetadata("default", "test_bank");
        tablesV2 = Lists.newLinkedList();
        for (TableMetaWithType t : tableMetasAddType) {
            TableMeta tableMeta = new TableMeta(t.getTABLE_CAT(), t.getTABLE_SCHEM(), t.getTABLE_NAME(),
                    t.getTABLE_TYPE(), t.getREMARKS(), t.getTYPE_CAT(), t.getTYPE_SCHEM(), t.getTYPE_NAME(),
                    t.getSELF_REFERENCING_COL_NAME(), t.getREF_GENERATION());
            tableMeta.setColumns(t.getColumns().stream()
                    .map(c -> new ColumnMeta(c.getTABLE_CAT(), c.getTABLE_SCHEM(), c.getTABLE_NAME(),
                            c.getCOLUMN_NAME(), c.getDATA_TYPE(), c.getTYPE_NAME(), c.getCOLUMN_SIZE(),
                            c.getBUFFER_LENGTH(), c.getDECIMAL_DIGITS(), c.getNUM_PREC_RADIX(), c.getNULLABLE(),
                            c.getREMARKS(), c.getCOLUMN_DEF(), c.getSQL_DATA_TYPE(), c.getSQL_DATETIME_SUB(),
                            c.getCHAR_OCTET_LENGTH(), c.getORDINAL_POSITION(), c.getIS_NULLABLE(), c.getSCOPE_CATLOG(),
                            c.getSCOPE_SCHEMA(), c.getSCOPE_TABLE(), c.getSOURCE_DATA_TYPE(), c.getIS_AUTOINCREMENT()))
                    .collect(Collectors.toList()));
            tablesV2.add(tableMeta);
        }
        Assert.assertEquals(JsonUtil.writeValueAsString(tablesV2), JsonUtil.writeValueAsString(tableMetas));
    }

    @Test
    public void testExposedColumnsProjectConfigByModel() throws Exception {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        // expose computed column
        {
            projectManager.updateProject("default", copyForWrite -> copyForWrite.getOverrideKylinProps()
                    .put("kylin.query.metadata.expose-computed-column", "true"));
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", "nmodel_basic_inner");

            List<ColumnMeta> factColumns;
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertTrue(getColumnNames(factColumns)
                    .containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT", "NEST1", "NEST2", "NEST3", "NEST4")));
        }

        // hide computed column
        {
            projectManager.updateProject("default", copyForWrite -> copyForWrite.getOverrideKylinProps()
                    .put("kylin.query.metadata.expose-computed-column", "false"));
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", "nmodel_basic_inner");

            List<ColumnMeta> factColumns;
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(columnDescs.length, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns)
                    .containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT", "NEST1", "NEST2", "NEST3", "NEST4")));
        }
    }

    @Test
    public void testExposedColumnsWhenPushdownDisabled() throws Exception {

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");

        //we have two projects: testproject2 and testproject1. different projects exposes different views of
        //table, depending on what ready cube it has.
        {
            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertFalse(tableSchemas.contains("metadata"));
            Assert.assertEquals(21, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(12, factColumns.size());
        }

        //disable the one ready cube
        {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
            NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);
            dataflow = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);

            Thread.sleep(1000);

            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertFalse(tableSchemas.contains("metadata"));
            Assert.assertEquals(21, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_MEASURE"));
        }

        // enable the ready cube
        {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
            NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);
            dataflow = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);

            Thread.sleep(1000);

            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertFalse(tableSchemas.contains("metadata"));
            Assert.assertEquals(21, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(12, factColumns.size());
        }
    }

    @Test
    public void testExposedColumnsByModelWhenPushdownDisabled() throws Exception {

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");

        //we have two projects: testproject2 and testproject1. different projects exposes different views of
        //table, depending on what ready cube it has.
        {
            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", "nmodel_basic_inner");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(2, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(8, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(12, factColumns.size());
        }

        //disable the one ready cube
        {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
            NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);
            dataflow = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);

            Thread.sleep(1000);

            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", "nmodel_basic_inner");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(2, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(8, tableNames.size());
        }

        // enable the ready cube
        {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
            NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);
            dataflow = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);

            Thread.sleep(1000);

            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", "nmodel_basic_inner");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(2, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(8, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(12, factColumns.size());
        }
    }

    @Test
    public void testExposedColumnsWhenPushdownEnabled() throws Exception {

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");

        //we have two projects: default and testproject1. different projects exposes different views of
        //table, depending on what model it has.
        {
            //check the default project
            final List<TableMetaWithType> tableMetas4default = queryService.getMetadataV2("default", null);

            schemasAndTables = getSchemasAndTables(tableMetas4default);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(21, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas4default);
            Assert.assertEquals(12, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
                    "_CC_LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "_CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "_CC_LEFTJOIN_BUYER_COUNTRY_ABBR", "_CC_LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }

        //add a new model with new cc
        {
            NDataModel dKapModel = makeModelWithMoreCC();
            modelManager.updateDataModelDesc(dKapModel);

            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);

            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(12, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
                    "_CC_LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "_CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "_CC_LEFTJOIN_BUYER_COUNTRY_ABBR", "_CC_LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }

        //remove a cc from model
        {
            NDataModel dKapModel = makeModelWithLessCC();
            modelManager.updateDataModelDesc(dKapModel);

            final List<TableMetaWithType> tableMetas4default = queryService.getMetadataV2("default", null);
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas4default);
            Assert.assertEquals(12, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
                    "_CC_LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "_CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "_CC_LEFTJOIN_BUYER_COUNTRY_ABBR", "_CC_LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }
    }

    private ColumnDesc[] findColumnDescs() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        tableMetadataManager.resetProjectSpecificTableDesc();
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        return tableDesc.getColumns();
    }

    private NDataModel makeModelWithLessCC() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = modelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Serializer<NDataModel> dataModelSerializer = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelSerializer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dataModelSerializer.serialize(model, new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel dKapModel = dataModelSerializer.deserialize(new DataInputStream(bais));
        dKapModel.setProject("default");

        dKapModel.getComputedColumnDescs().remove(dKapModel.getComputedColumnDescs().size() - 1);
        dKapModel.setMvcc(model.getMvcc());
        return dKapModel;
    }

    private NDataModel makeModelWithMoreCC() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = modelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Serializer<NDataModel> dataModelSerializer = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelSerializer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dataModelSerializer.serialize(model, new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel dKapModel = dataModelSerializer.deserialize(new DataInputStream(bais));
        dKapModel.setProject("default");

        String newCCStr = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"tableAlias\": \"TEST_KYLIN_FACT\",\n" + "      \"columnName\": \"DEAL_YEAR_PLUS_ONE\",\n"
                + "      \"expression\": \"year(TEST_KYLIN_FACT.CAL_DT)+1\",\n" + "      \"datatype\": \"integer\",\n"
                + "      \"comment\": \"test use\"\n" + "    }";
        ComputedColumnDesc computedColumnDesc = JsonUtil.readValue(newCCStr, ComputedColumnDesc.class);
        dKapModel.getComputedColumnDescs().add(computedColumnDesc);
        dKapModel.setMvcc(model.getMvcc());
        return dKapModel;
    }

    private Pair<Set<String>, Set<String>> getSchemasAndTables(List<TableMetaWithType> tableMetas) {
        Set<String> tableSchemas = Sets.newHashSet();
        Set<String> tableNames = Sets.newHashSet();
        for (TableMetaWithType tableMetaWithType : tableMetas) {
            tableSchemas.add(tableMetaWithType.getTABLE_SCHEM());
            tableNames.add(tableMetaWithType.getTABLE_NAME());
        }

        return Pair.newPair(tableSchemas, tableNames);
    }

    private List<ColumnMeta> getFactColumns(List<TableMetaWithType> tableMetas) {
        Optional<TableMetaWithType> factTable = tableMetas.stream()
                .filter(tableMetaWithType -> tableMetaWithType.getTABLE_NAME().equals("TEST_KYLIN_FACT")).findFirst();
        Assert.assertTrue(factTable.isPresent());
        return factTable.get().getColumns();
    }

    private Set<String> getColumnNames(List<ColumnMeta> columns) {
        return columns.stream().map(ColumnMeta::getCOLUMN_NAME).collect(Collectors.toSet());
    }

    @Test
    public void testQueryWithConstants() throws Exception {
        String sql = "select price from test_kylin_fact where 1 <> 1";
        stubQueryConnection(sql, "default");

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        SQLResponse response = queryService.queryWithCache(request);
        Assert.assertEquals("CONSTANTS", response.getEngineType());
    }

    @Test
    public void testQueryWithEmptyLayout() throws Exception {
        String sql = "select price*item_count from test_kylin_fact where cal_dt = '2020-01-01' limit 100";
        stubQueryConnection(sql, "default");
        mockOLAPContextForEmptyLayout();

        SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        SQLResponse response = queryService.queryWithCache(request);
        Assert.assertEquals(1, response.getNativeRealizations().size());
        NativeQueryRealization realization = response.getNativeRealizations().get(0);
        Assert.assertEquals("mock_model_alias1", realization.getModelAlias());
        Assert.assertNull(realization.getLayoutId());
        Assert.assertNull(realization.getIndexType());
    }

    @Test
    public void testSaveQuery() throws IOException {
        Query query = new Query("test", "default", "test_sql", "test_description");
        queryService.saveQuery("admin", "default", query);
        QueryService.QueryRecord queryRecord = queryService.getSavedQueries("admin", "default");
        Assert.assertEquals(1, queryRecord.getQueries().size());
        Assert.assertEquals("test", queryRecord.getQueries().get(0).getName());

        query.setSql("test_sql_2");
        try {
            queryService.saveQuery("admin", "default", query);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertEquals("Query named \"test\" already exists. Please check and try again.", ex.getMessage());
        }

        queryRecord = queryService.getSavedQueries("admin", "default");
        Assert.assertEquals(1, queryRecord.getQueries().size());
        Assert.assertEquals("test", queryRecord.getQueries().get(0).getName());
    }

    @Test
    public void testSaveLargeQuery() throws IOException {
        for (int i = 0; i < 10; i++) {
            Query query = new Query("test-" + i, "default", StringUtils.repeat("abc", 10000), "test_description");
            queryService.saveQuery("admin", "default", query);
        }
        QueryService.QueryRecord queryRecord = queryService.getSavedQueries("admin", "default");
        Assert.assertEquals(10, queryRecord.getQueries().size());
        for (Query query : queryRecord.getQueries()) {
            Assert.assertEquals(StringUtils.repeat("abc", 10000), query.getSql());
        }
    }

    @Test
    public void testCacheSignature() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val layoutId = 1000001L;
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);

        SQLResponse response = new SQLResponse();
        response.setNativeRealizations(Lists.newArrayList(
                new NativeQueryRealization(modelId, layoutId, QueryMetricsContext.AGG_INDEX, Lists.newArrayList())));
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        String signature = QueryCacheSignatureUtil.createCacheSignature(response, project);
        Assert.assertEquals(
                String.valueOf(
                        dataflowManager.getDataflow(modelId).getLastSegment().getLayout(layoutId).getCreateTime()),
                signature.split(",")[1].split(";")[0]);
        response.setSignature(signature);
        dataflowManager.updateDataflow(modelId, copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCacheSignatureWhenModelOffline() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val layoutId = 1000001L;
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        SQLResponse response = new SQLResponse();
        response.setNativeRealizations(Lists.newArrayList(
                new NativeQueryRealization(modelId, layoutId, QueryMetricsContext.AGG_INDEX, Lists.newArrayList())));
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));

        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(response, project));
        //let model offline
        dataflowManager.updateDataflowStatus("89af4ee2-2cdb-4b07-b39e-4c29856309aa", RealizationStatusEnum.OFFLINE);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCacheSignatureWhenTableModified() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val layoutId = 1000001L;
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        SQLResponse response = new SQLResponse();
        response.setNativeRealizations(Lists.newArrayList(
                new NativeQueryRealization(modelId, layoutId, QueryMetricsContext.AGG_INDEX, Lists.newArrayList())));
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));

        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(response, project));
        //modify table
        dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getModel().getRootFactTable().getTableDesc()
                .setLastModified(1);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testAddColsToTblMetaWithSpecialCharacter() {
        Map<QueryService.TableMetaIdentify, TableMetaWithType> tblMap = new HashMap<>();
        Map<QueryService.ColumnMetaIdentify, ColumnMetaWithType> columnMetaWithTypeMap = new HashMap<>();
        tblMap.put(new QueryService.TableMetaIdentify("default", "city"), new TableMetaWithType());
        //        column name contain #
        columnMetaWithTypeMap.put(new QueryService.ColumnMetaIdentify("default", "city", "n#a#me"),
                new ColumnMetaWithType(null, null, null, null, 0, null, 0, 0, 0, 0, 0, null, null, 0, 0, 0, 0, null,
                        null, null, null, (short) 0, null));
        QueryService.addColsToTblMeta(tblMap, columnMetaWithTypeMap);
    }

    @Test
    public void testQueryWithCacheSignatureNotExpired() throws Exception {

        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelAlias = "nmodel_basic";
        long layoutId = 1000001L;
        final String project = "default";
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);

        final String sql = "select * from success_table_1";
        stubQueryConnection(sql, project);
        mockOLAPContextWithOneModelInfo(modelId, modelAlias, layoutId);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        // case of not hitting cache
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        final SQLResponse firstSuccess = queryService.queryWithCache(request);

        // case of hitting cache
        final SQLResponse secondSuccess = queryService.queryWithCache(request);
        Assert.assertTrue(secondSuccess.isStorageCacheUsed());
        Assert.assertEquals(1, secondSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, secondSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals("nmodel_basic", secondSuccess.getNativeRealizations().get(0).getModelAlias());

        // modify model name
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId).getModel()
                .setAlias("model_new");
        final SQLResponse secondSuccess1 = queryService.queryWithCache(request);
        Assert.assertTrue(secondSuccess1.isStorageCacheUsed());
        Assert.assertEquals(1, secondSuccess1.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX,
                secondSuccess1.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals("model_new", secondSuccess1.getNativeRealizations().get(0).getModelAlias());
    }

    @Test
    public void testQueryWithCacheSignatureExpired() throws Exception {

        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelAlias = "nmodel_basic";
        long layoutId = 1000001L;
        final String project = "default";
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);

        final String sql = "select * from success_table_2";
        stubQueryConnection(sql, project);
        mockOLAPContextWithOneModelInfo(modelId, modelAlias, layoutId);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        // case of not hitting cache
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        final SQLResponse firstSuccess = queryService.queryWithCache(request);

        dataflowManager.updateDataflow(modelId, copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        // case of cache expired
        final SQLResponse thirdSuccess = queryService.queryWithCache(request);
        Assert.assertFalse(thirdSuccess.isStorageCacheUsed());
        Assert.assertEquals(1, thirdSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, thirdSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals(modelAlias, thirdSuccess.getNativeRealizations().get(0).getModelAlias());
    }

    @Test
    public void testQueryContextWhenHitCache() throws Exception {
        final String project = "default";
        final String sql = "select * from success_table_3";

        stubQueryConnection(sql, project);
        mockOLAPContext();

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        SQLResponse sqlResponse = queryService.doQueryWithCache(request);
        Assert.assertNull(QueryContext.current().getEngineType());
        Assert.assertEquals(-1, QueryContext.current().getMetrics().getTotalScanBytes());
        Assert.assertEquals(-1, QueryContext.current().getMetrics().getTotalScanRows());
        Assert.assertEquals(0, QueryContext.current().getMetrics().getResultRowCount());

        sqlResponse.setScanBytes(Lists.newArrayList(1024L));
        sqlResponse.setScanRows(Lists.newArrayList(10000L));
        sqlResponse.setResultRowCount(500);
        queryCacheManager.cacheSuccessQuery(request, sqlResponse);

        queryService.doQueryWithCache(request);
        Assert.assertEquals("NATIVE", QueryContext.current().getEngineType());
        Assert.assertEquals(1024, QueryContext.current().getMetrics().getTotalScanBytes());
        Assert.assertEquals(10000, QueryContext.current().getMetrics().getTotalScanRows());
        Assert.assertEquals(500, QueryContext.current().getMetrics().getResultRowCount());

        queryCacheManager.clearQueryCache(request);
    }

    @Test
    public void testAnswerByWhenQueryFailed() throws Exception {
        final String project = "default";
        final String constantQueryFailSql = "select * from success_table_3";

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(constantQueryFailSql);
        request.setQueryId("testAnswerByWhenQueryFailed");

        QueryMetricsContext.start(request.getQueryId(), "");

        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        SQLResponse sqlResponse = queryService.doQueryWithCache(request);
        Assert.assertTrue(sqlResponse.isException());
        Assert.assertTrue(QueryContext.current().getMetrics().isException());
        Assert.assertFalse(QueryContext.current().getQueryTagInfo().isPushdown());
        Assert.assertFalse(QueryContext.current().getQueryTagInfo().isConstantQuery());
    }

    @Test
    @Ignore
    public void testQueryWithResultRowCountBreaker() {
        final String sql = "select * from success_table_2";
        final String project = "default";
        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        final SQLResponse response = Mockito.mock(SQLResponse.class);
        Mockito.doReturn(2L).when(response).getResultRowCount();
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        getTestConfig().setProperty("kylin.circuit-breaker.threshold.query-result-row-count", "1");

        Mockito.doReturn(response).when(queryService).queryAndUpdateCache(Mockito.any(SQLRequest.class),
                Mockito.any(KylinConfig.class));
        try {
            getTestConfig().setProperty("kylin.server.mode", "job");
            NCircuitBreaker.start(KapConfig.wrap(getTestConfig()));
            queryService.doQueryWithCache(request);
        } catch (KylinException e) {
            Assert.assertEquals("Job node is unavailable for queries. Please select a query node.", e.getMessage());
        }

        try {
            getTestConfig().setProperty("kylin.server.mode", "query");
            NCircuitBreaker.start(KapConfig.wrap(getTestConfig()));
            val queryWithCache = queryService.doQueryWithCache(request);
            Assert.assertTrue(queryWithCache.isException());
        } catch (Exception e) {
            Assert.fail();
        } finally {
            NCircuitBreaker.stop();
        }
    }

    @Test
    public void testQueryWithSpecificQueryId() throws Exception {
        final String sql = "select * from test";
        final String project = "default";
        final String queryId = RandomUtil.randomUUIDStr();
        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        request.setQueryId(queryId);

        final SQLResponse response = queryService.queryWithCache(request);
        Assert.assertEquals(queryId, response.getQueryId());
    }

    @Test
    public void testQueryOfTrace() {
        final String sql = "select * from test";
        final String project = "default";
        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        request.setQueryId(RandomUtil.randomUUIDStr());
        QueryContext.currentTrace().startSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB);
        final SQLResponse response = queryService.queryWithCache(request);
        Assert.assertEquals(SPARK_JOB_EXECUTION, response.getTraces().get(0).getName());

        QueryContext.currentTrace().clear();
        QueryContext.currentTrace().startSpan(EXECUTION);
        final SQLResponse response1 = queryService.queryWithCache(request);
        Assert.assertEquals(EXECUTION, response1.getTraces().get(0).getName());
    }

    @Test
    public void testQueryLogMatch() {
        final String sql = "-- This is comment" + '\n' + "select * from test";
        final String project = "default";
        final String tag = "tagss";
        final String pushDownForced = "false";
        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        request.setUser_defined_tag(tag);
        HashMap<String, String> backdoorToggles = Maps.newHashMap();
        backdoorToggles.put("DEBUG_TOGGLE_HTRACE_ENABLED", "false");
        request.setBackdoorToggles(backdoorToggles);
        request.setUserAgent("Chrome/89.0.4389.82 Safari/537.36");

        final SQLResponse response = queryService.queryWithCache(request);

        // Current QueryContext will be reset in doQueryWithCache
        QueryContext.current().setUserSQL(sql);
        String log = queryService.logQuery(request, response);
        //
        final int groupCnt = 33;
        String matchNewLine = "\\n";
        String s = "(?s)[=]+\\[QUERY\\][=]+.*Query Id:\\s(.*?)" + matchNewLine + "SQL:\\s(.*?)" + matchNewLine
                + "User:\\s(.*?)" + matchNewLine + "Success:\\s(.*?)" + matchNewLine + "Duration:\\s(.*?)"
                + matchNewLine + "Project:\\s(.*?)" + matchNewLine + "Realization Names:\\s\\[(.*?)\\]" + matchNewLine
                + "Index Layout Ids:\\s\\[(.*?)\\]" + matchNewLine + "Is Partial Match Model:\\s\\[(.*?)\\]"
                + matchNewLine + "Scan rows:\\s(.*?)" + matchNewLine + "Total Scan rows:\\s(.*?)" + matchNewLine
                + "Scan bytes:\\s(.*?)" + matchNewLine + "Total Scan Bytes:\\s(.*?)" + matchNewLine
                + "Result Row Count:\\s(.*?)" + matchNewLine + "Shuffle partitions:\\s(.*?)" + matchNewLine
                + "Accept Partial:\\s(.*?)" + matchNewLine + "Is Partial Result:\\s(.*?)" + matchNewLine
                + "Hit Exception Cache:\\s(.*?)" + matchNewLine + "Storage Cache Used:\\s(.*?)" + matchNewLine
                + "Storage Cache Type:\\s(.*?)" + matchNewLine + "Is Query Push-Down:\\s(.*?)" + matchNewLine
                + "Is Prepare:\\s(.*?)" + matchNewLine + "Is Timeout:\\s(.*?)" + matchNewLine + "Trace URL:\\s(.*?)"
                + matchNewLine + "Time Line Schema:\\s(.*?)" + matchNewLine + "Time Line:\\s(.*?)" + matchNewLine
                + "Message:\\s(.*?)" + matchNewLine + "User Defined Tag:\\s(.*?)" + matchNewLine
                + "Is forced to Push-Down:\\s(.*?)" + matchNewLine + "User Agent:\\s(.*?)" + matchNewLine
                + "Back door toggles:\\s(.*?)" + matchNewLine + "Scan Segment Count:\\s(.*?)" + matchNewLine
                + "Scan File Count:\\s(.*?)" + matchNewLine + "[=]+\\[QUERY\\][=]+.*";
        Pattern pattern = Pattern.compile(s);
        Matcher matcher = pattern.matcher(log);

        Assert.assertTrue(matcher.find());
        for (int i = 0; i < groupCnt; i++)
            Assert.assertNotNull(matcher.group(i));
        Assert.assertEquals(groupCnt, matcher.groupCount());

        Assert.assertEquals(QueryContext.current().getQueryId(), matcher.group(1));
        Assert.assertEquals(sql, matcher.group(2));
        Assert.assertEquals(project, matcher.group(6));
        Assert.assertFalse(Boolean.parseBoolean(matcher.group(4)));
        Assert.assertEquals("null", matcher.group(24)); //Trace URL
        Assert.assertEquals(tag, matcher.group(28));
        Assert.assertEquals(pushDownForced, matcher.group(29));
        Assert.assertEquals("Chrome/89.0.4389.82 Safari/537.36", matcher.group(30));
        Assert.assertEquals("{DEBUG_TOGGLE_HTRACE_ENABLED=false}", matcher.group(31));
    }

    @Test
    public void testQueryWithParam() {
        final String sql = "select * from test where col1 = ?;";
        final String sqlFullTextString = "select * from test where col1 = ?";
        String filledSql = "select * from test where col1 = 'value1'";
        final String project = "default";
        final PrepareSqlRequest request = new PrepareSqlRequest();
        request.setProject(project);
        request.setSql(sql);
        PrepareSqlStateParam[] params = new PrepareSqlStateParam[1];
        params[0] = new PrepareSqlStateParam(String.class.getCanonicalName(), "value1");
        request.setParams(params);

        final SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        response.setEngineType("NATIVE");

        final QueryContext queryContext = QueryContext.current();

        overwriteSystemProp("kylin.query.replace-dynamic-params-enabled", "true");
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        queryService.doQueryWithCache(request);
        Assert.assertEquals(sqlFullTextString, queryContext.getUserSQL());
        Assert.assertEquals(filledSql, queryContext.getMetrics().getCorrectedSql());

        overwriteSystemProp("kylin.query.replace-dynamic-params-enabled", "false");
        queryService.doQueryWithCache(request);
        Assert.assertEquals(sqlFullTextString, queryContext.getUserSQL());
        Assert.assertEquals(filledSql, queryContext.getMetrics().getCorrectedSql());

        queryContext.getMetrics().setCorrectedSql(filledSql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, Object> influxdbFields = getInfluxdbFields(metricsContext);
        Assert.assertEquals(filledSql, influxdbFields.get(QueryHistory.SQL_TEXT));
        QueryMetricsContext.reset();
    }

    @Test
    public void testQueryIDShouldBeDifferentAfterReset() {
        QueryContext curOld = QueryContext.current();
        QueryContext.reset();
        QueryContext curNew = QueryContext.current();
        Pattern uuid_p = Pattern
                .compile("([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}){1}");
        Assert.assertNotNull(curNew);
        Assert.assertTrue(StringUtils.isNotEmpty(curNew.getQueryId()));

        Matcher matcher = uuid_p.matcher(curNew.getQueryId());
        Assert.assertTrue(matcher.find());

        Assert.assertNotEquals(curOld.getQueryId(), curNew.getQueryId());
    }

    @Test
    public void testMetaData() throws IOException {
        final List<TableMeta> tableMetas = queryService.getMetadata("default");
        // TEST_MEASURE table has basically all possible column types
        String metaString = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_MEASURE")).findFirst().get().toString();

        File expectedMetaFile = new File("src/test/resources/ut_table_meta/defaultTableMetas");
        String expectedMetaString = FileUtils.readFileToString(expectedMetaFile);
        Assert.assertEquals(expectedMetaString, metaString);
    }

    @Test
    public void testMetaDataWhenSchemaCacheEnable() throws IOException {
        updateProjectConfig("default", "kylin.query.schema-cache-enabled", "true");
        final List<TableMeta> tableMetas = queryService.getMetadata("default");
        // TEST_MEASURE table has basically all possible column types
        String metaString = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_MEASURE")).findFirst().get().toString();

        File expectedMetaFile = new File("src/test/resources/ut_table_meta/defaultTableMetas");
        String expectedMetaString = FileUtils.readFileToString(expectedMetaFile);
        Assert.assertEquals(expectedMetaString, metaString);
    }

    @Test
    public void testMetaDataColumnCaseSensitive() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.source.name-case-sensitive-enabled", "true");
        final List<TableMeta> tableMetas = queryService.getMetadata("default");
        TableMeta tableToCheck = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_ACCOUNT")).findFirst().get();
        ColumnMeta columnToCheck = tableToCheck.getColumns().stream()
                .filter(c -> c.getCOLUMN_NAME().equalsIgnoreCase("ACCOUNT_ID")).findFirst().get();
        Assert.assertEquals(columnToCheck.getCOLUMN_NAME(), "account_id");
    }

    @Test
    public void testMetaDataColumnCaseNotSensitive() throws IOException {
        final List<TableMeta> tableMetas = queryService.getMetadata("default");
        TableMeta tableToCheck = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_ACCOUNT")).findFirst().get();
        ColumnMeta columnToCheck = tableToCheck.getColumns().stream()
                .filter(c -> c.getCOLUMN_NAME().equalsIgnoreCase("ACCOUNT_ID")).findFirst().get();
        Assert.assertEquals(columnToCheck.getCOLUMN_NAME(), "ACCOUNT_ID");
    }

    @Test
    public void testMetaDataV2ColumnCaseSensitive() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.source.name-case-sensitive-enabled", "true");
        final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);
        TableMeta tableToCheck = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_ACCOUNT")).findFirst().get();
        ColumnMeta columnToCheck = tableToCheck.getColumns().stream()
                .filter(c -> c.getCOLUMN_NAME().equalsIgnoreCase("ACCOUNT_ID")).findFirst().get();
        Assert.assertEquals(columnToCheck.getCOLUMN_NAME(), "account_id");
    }

    @Test
    public void testMetaDataV2ColumnCaseNotSensitive() throws IOException {
        final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);
        TableMeta tableToCheck = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_ACCOUNT")).findFirst().get();
        ColumnMeta columnToCheck = tableToCheck.getColumns().stream()
                .filter(c -> c.getCOLUMN_NAME().equalsIgnoreCase("ACCOUNT_ID")).findFirst().get();
        Assert.assertEquals(columnToCheck.getCOLUMN_NAME(), "ACCOUNT_ID");
    }

    @Test
    public void testMetaDataV2() throws IOException {
        final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);
        // TEST_MEASURE table has basically all possible column types
        String metaString = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_MEASURE")).findFirst().get().toString();

        File expectedMetaFile = new File("src/test/resources/ut_table_meta/defaultTableMetasV2");
        String expectedMetaString = FileUtils.readFileToString(expectedMetaFile);
        Assert.assertEquals(expectedMetaString, metaString);
    }

    @Test
    //ref KE-12803
    public void testDeepCopy() {
        final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default", null);
        tableMetas.stream()
                .map(tableMetaWithType -> JsonUtil.deepCopyQuietly(tableMetaWithType, TableMetaWithType.class))
                .collect(Collectors.toList());
    }

    @Test
    //reference KE-8052
    public void testQueryWithConstant() throws SQLException {
        doTestQueryWithConstant("select current_timestamp");
        doTestQueryWithConstant("select 1,2,3,4,5");
        doTestQueryWithConstant("select max(1) from TEST_ACCOUNT inner join TEST_MEASURE "
                + "on TEST_ACCOUNT.ACCOUNT_ID = TEST_MEASURE.ID1");
    }

    private void doTestQueryWithConstant(String testSql) {
        SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(testSql);
        request.setQueryId(RandomUtil.randomUUIDStr());

        Predicate<SQLResponse> scannedRows = (s -> s.getTotalScanRows() == 0);
        Predicate<SQLResponse> scannedBytes = (s -> s.getTotalScanBytes() == 0);

        final SQLResponse response = queryService.queryWithCache(request);
        Assert.assertTrue(scannedRows.and(scannedBytes).test(response));
    }

    @Test
    //reference KE-8052
    public void testQueryWithScanBytesAndRows() {
        long defaultValue = QueryContext.DEFAULT_NULL_SCANNED_DATA;

        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setScanRows(Lists.newArrayList(1L, 2L));
        sqlResponse.setScanBytes(Lists.newArrayList(2L, 3L));

        Assert.assertEquals(3L, sqlResponse.getTotalScanRows());
        Assert.assertEquals(5L, sqlResponse.getTotalScanBytes());

        SQLResponse sqlResponseNull = new SQLResponse();
        sqlResponseNull.setScanRows(null);
        sqlResponseNull.setScanBytes(null);

        Assert.assertEquals(sqlResponseNull.getTotalScanRows(), defaultValue);
        Assert.assertEquals(sqlResponseNull.getTotalScanBytes(), defaultValue);

        SQLResponse sqlResponseEmpty = new SQLResponse();

        sqlResponseEmpty.setScanRows(Lists.newArrayList());
        sqlResponseEmpty.setScanBytes(Lists.newArrayList());

        Assert.assertEquals(0, sqlResponseEmpty.getTotalScanRows());
        Assert.assertEquals(0, sqlResponseEmpty.getTotalScanBytes());
    }

    @Test
    public void testGetMetadataV2WithBrokenModels() {
        String modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        List<TableMetaWithType> metaWithTypeList = queryService.getMetadataV2("default", null);
        boolean noFactTableType = metaWithTypeList.stream()
                .filter(tableMetaWithType -> "TEST_MEASURE".equals(tableMetaWithType.getTABLE_NAME())).findFirst().get()
                .getTYPE().isEmpty();
        Assert.assertFalse(noFactTableType);

        // fact table is broken
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = modelManager.getDataModelDesc(modelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        modelManager.updateDataBrokenModelDesc(brokenModel);

        metaWithTypeList = queryService.getMetadataV2("default", null);
        noFactTableType = metaWithTypeList.stream()
                .filter(tableMetaWithType -> "TEST_MEASURE".equals(tableMetaWithType.getTABLE_NAME())).findFirst().get()
                .getTYPE().isEmpty();
        Assert.assertTrue(noFactTableType);
    }

    @Test
    public void testGetMetadataV2WhenSchemaCacheEnable() {
        updateProjectConfig("default", "kylin.query.schema-cache-enabled", "true");
        List<TableMetaWithType> metaWithTypeList = queryService.getMetadataV2("default", null);
    }

    @Test
    public void testGetMetadataV2ByModelWithProjectContainBrokenModels() {
        String modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        List<TableMetaWithType> metaWithTypeList = queryService.getMetadataV2("default", "nmodel_basic_inner");
        Assert.assertTrue(metaWithTypeList.stream()
                .filter(tableMetaWithType -> "TEST_MEASURE".equals(tableMetaWithType.getTABLE_NAME()))
                .collect(Collectors.toList()).isEmpty());

        // fact table is broken
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = modelManager.getDataModelDesc(modelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        modelManager.updateDataBrokenModelDesc(brokenModel);

        metaWithTypeList = queryService.getMetadataV2("default", "nmodel_basic_inner");
        Assert.assertTrue(metaWithTypeList.stream()
                .filter(tableMetaWithType -> "TEST_MEASURE".equals(tableMetaWithType.getTABLE_NAME()))
                .collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void testGetMetadataV2ByBrokenModel() {
        String modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        List<TableMetaWithType> metaWithTypeList = queryService.getMetadataV2("default", "nmodel_full_measure_test");
        boolean noFactTableType = metaWithTypeList.stream()
                .filter(tableMetaWithType -> "TEST_MEASURE".equals(tableMetaWithType.getTABLE_NAME())).findFirst().get()
                .getTYPE().isEmpty();
        Assert.assertFalse(noFactTableType);

        // fact table is broken
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = modelManager.getDataModelDesc(modelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        modelManager.updateDataBrokenModelDesc(brokenModel);

        metaWithTypeList = queryService.getMetadataV2("default", "nmodel_full_measure_test");
        Assert.assertTrue(metaWithTypeList.stream()
                .filter(tableMetaWithType -> "TEST_MEASURE".equals(tableMetaWithType.getTABLE_NAME()))
                .collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void testExecuteAsUserSwitchOff() {
        overwriteSystemProp("kylin.query.query-with-execute-as", "false");
        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setExecuteAs("unknown");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Configuration item \"kylin.query.query-with-execute-as\" "
                + "is not enabled. So you cannot use the \"executeAs\" parameter now");
        queryService.queryWithCache(request);
    }

    @Test
    public void testExecuteAsUserServiceAccountAccessDenied() {
        try {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("testuser", "testuser", Constant.ROLE_MODELER));
            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql("select 2");
            request.setExecuteAs("ADMIN");
            getTestConfig().setProperty("kylin.query.query-with-execute-as", "true");
            request.setQueryId(RandomUtil.randomUUIDStr());
            thrown.expect(KylinException.class);
            thrown.expectMessage("User [testuser] does not have permissions for all tables, rows, "
                    + "and columns in the project [default] and cannot use the executeAs parameter");
            queryService.queryWithCache(request);
        } finally {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        }
    }

    @Test
    public void testExecuteAsADMIN() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql("select 2");
        getTestConfig().setProperty("kylin.query.query-with-execute-as", "true");
        request.setQueryId(RandomUtil.randomUUIDStr());
        request.setExecuteAs("ADMIN");
        queryService.queryWithCache(request);
    }

    @Test
    public void testExecuteAsProjectAdmin() {
        try {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("prjAdmin", "prjAdmin", Constant.ROLE_MODELER));
            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql("select 2");
            getTestConfig().setProperty("kylin.query.query-with-execute-as", "true");
            request.setQueryId(RandomUtil.randomUUIDStr());
            request.setExecuteAs("ADMIN");
            ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getProject("default");
            AclEntity projectAE = AclEntityFactory.createAclEntity(AclEntityType.PROJECT_INSTANCE,
                    projectInstance.getUuid());
            AclServiceTest.MockAclEntity userAE = new AclServiceTest.MockAclEntity("prjAdmin");
            MutableAclRecord projectAcl = (MutableAclRecord) aclService.createAcl(new ObjectIdentityImpl(projectAE));
            aclService.createAcl(new ObjectIdentityImpl(userAE));
            Map<Sid, Permission> map = new HashMap<>();
            Sid sidUser = accessService.getSid("prjAdmin", true);
            map.put(sidUser, ADMINISTRATION);
            queryService.getManager(AclManager.class).batchUpsertAce(projectAcl, map);
            MutableAclRecord ace = AclPermissionUtil.getProjectAcl("default");
            queryService.queryWithCache(request);
        } finally {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        }
    }

    @Test
    public void testExecuteAsNormalUser() {
        try {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("testuser", "testuser", Constant.ROLE_MODELER));
            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql("select 2");
            request.setExecuteAs("ADMIN");
            getTestConfig().setProperty("kylin.query.query-with-execute-as", "true");
            request.setQueryId(RandomUtil.randomUUIDStr());
            AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), "default");
            AclTCR u1a1 = new AclTCR();
            manager.updateAclTCR(u1a1, "testuser", true);
            queryService.queryWithCache(request);
        } finally {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        }
    }

    @Test
    public void testExecuteAsUserAccessDenied() {
        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql("select 2");
        getTestConfig().setProperty("kylin.query.query-with-execute-as", "true");
        request.setQueryId(RandomUtil.randomUUIDStr());
        userService.createUser(new ManagedUser("testuser", "KYLIN", false, Arrays.asList()));
        userService.userExists("testuser");
        request.setExecuteAs("testuser");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Access is denied.");
        queryService.queryWithCache(request);
    }

    @Test
    public void testQueryWithAdminPermission() {
        QueryService queryService = Mockito.spy(new QueryService());
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setExecuteAs("ADMIN");
        sqlRequest.setProject("default");
        sqlRequest.setSql("select 1");

        overwriteSystemProp("kylin.query.security.acl-tcr-enabled", "true");
        // role admin
        {
            Mockito.doReturn(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("ROLE_ADMIN"), false)).when(queryService)
                    .getExecuteAclInfo("default", "ADMIN");
            Assert.assertTrue(
                    queryService.isACLDisabledOrAdmin("default", queryService.getExecuteAclInfo("default", "ADMIN")));
        }

        // project admin permission
        {
            Mockito.doReturn(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("FOO"), true)).when(queryService)
                    .getExecuteAclInfo("default", "ADMIN");
            Assert.assertTrue(
                    queryService.isACLDisabledOrAdmin("default", queryService.getExecuteAclInfo("default", "ADMIN")));
        }

        // normal user
        {
            Mockito.doReturn(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("FOO"), false)).when(queryService)
                    .getExecuteAclInfo("default", "ADMIN");
            Assert.assertFalse(
                    queryService.isACLDisabledOrAdmin("default", queryService.getExecuteAclInfo("default", "ADMIN")));
        }

        overwriteSystemProp("kylin.query.security.acl-tcr-enabled", "false");
        // acl disabled
        {
            Mockito.doReturn(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("FOO"), false)).when(queryService)
                    .getExecuteAclInfo("default", "ADMIN");
            Assert.assertTrue(
                    queryService.isACLDisabledOrAdmin("default", queryService.getExecuteAclInfo("default", "ADMIN")));
        }
    }

    @Test
    public void testQuerySelectStar() {
        overwriteSystemProp("kylin.query.return-empty-result-on-select-star", "true");
        String[] select_star_sqls = { "select * from TEST_KYLIN_FACT", "select * from TEST_ACCOUNT",
                "select * from TEST_KYLIN_FACT inner join TEST_ACCOUNT on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID" };
        for (String sql : select_star_sqls) {
            SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setQueryId(RandomUtil.randomUUIDStr());

            final SQLResponse response = queryService.queryWithCache(request);
            Assert.assertEquals(0, response.getResultRowCount());
        }
    }

    @Test
    public void testTableauIntercept() throws Exception {
        List<String> sqlList = Files.walk(Paths.get("./src/test/resources/query/tableau_probing"))
                .filter(file -> Files.isRegularFile(file)).map(path -> {
                    try {
                        String sql = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                        return new RawSqlParser(sql).parse().getStatementString();
                    } catch (Exception e) {
                        return null;
                    }
                }).filter(sql -> sql != null || sql.startsWith("SELECT")).collect(Collectors.toList());

        for (String sql : sqlList) {
            SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setQueryId(RandomUtil.randomUUIDStr());

            final SQLResponse response = queryService.query(request);
            Assert.assertNotNull(response);
            Assert.assertEquals(SPARK_JOB_EXECUTION, QueryContext.currentTrace().getLastSpan().get().getName());
        }
    }

    @Test
    public void testQueryContextWithFusionModel() throws Exception {
        final String project = "streaming_test";

        {
            final String sql = "select count(*) from SSB_STREAMING";

            stubQueryConnection(sql, project);
            mockOLAPContextWithHybrid();

            final SQLRequest request = new SQLRequest();
            request.setProject(project);
            request.setSql(sql);
            Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
            SQLResponse sqlResponse = queryService.doQueryWithCache(request);

            Assert.assertEquals(2, sqlResponse.getNativeRealizations().size());

            Assert.assertEquals("4965c827-fbb4-4ea1-a744-3f341a3b030d",
                    sqlResponse.getNativeRealizations().get(0).getModelId());
            Assert.assertEquals((Long) 10001L, sqlResponse.getNativeRealizations().get(0).getLayoutId());
            Assert.assertEquals("cd2b9a23-699c-4699-b0dd-38c9412b3dfd",
                    sqlResponse.getNativeRealizations().get(1).getModelId());
            Assert.assertEquals((Long) 20001L, sqlResponse.getNativeRealizations().get(1).getLayoutId());

            Assert.assertTrue(sqlResponse.getNativeRealizations().get(0).isStreamingLayout());
            Assert.assertFalse(sqlResponse.getNativeRealizations().get(1).isStreamingLayout());
        }
        {
            final String sql = "select count(1) from SSB_STREAMING";

            stubQueryConnection(sql, project);
            mockOLAPContextWithBatchPart();

            final SQLRequest request = new SQLRequest();
            request.setProject(project);
            request.setSql(sql);
            Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
            SQLResponse sqlResponse = queryService.doQueryWithCache(request);

            Assert.assertEquals(1, sqlResponse.getNativeRealizations().size());
            Assert.assertFalse(sqlResponse.getNativeRealizations().get(0).isStreamingLayout());
            Assert.assertEquals("cd2b9a23-699c-4699-b0dd-38c9412b3dfd",
                    sqlResponse.getNativeRealizations().get(0).getModelId());
        }
    }

    @Test
    public void testQueryContextWithStreamingModel() throws Exception {
        final String project = "demo";
        final String sql = "select count(*) from SSB_STREAMING";

        stubQueryConnection(sql, project);
        mockOLAPContextWithStreaming();

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        SQLResponse sqlResponse = queryService.doQueryWithCache(request);

        Assert.assertEquals(1, sqlResponse.getNativeRealizations().size());

        Assert.assertEquals("4965c827-fbb4-4ea1-a744-3f341a3b030d",
                sqlResponse.getNativeRealizations().get(0).getModelId());
        Assert.assertEquals((Long) 10001L, sqlResponse.getNativeRealizations().get(0).getLayoutId());

        Assert.assertTrue(sqlResponse.getNativeRealizations().get(0).isStreamingLayout());
    }

    @Test
    public void testQueryWithParamWhenTransformWithToSubQuery() {
        overwriteSystemProp("kylin.query.transformers",
                "io.kyligence.kap.query.util.ReplaceStringWithVarchar,org.apache.kylin.query.util.PowerBIConverter,org.apache.kylin.query.util.DefaultQueryTransformer,org.apache.kylin.query.util.EscapeTransformer,org.apache.kylin.query.util.ConvertToComputedColumn,org.apache.kylin.query.util.KeywordDefaultDirtyHack,org.apache.kylin.query.security.RowFilter,org.apache.kylin.query.util.WithToSubQueryTransformer");
        PrepareSqlStateParam[] params1 = new PrepareSqlStateParam[2];
        params1[0] = new PrepareSqlStateParam(Double.class.getCanonicalName(), "123.1");
        params1[1] = new PrepareSqlStateParam(Integer.class.getCanonicalName(), "123");
        String originSql1 = "with t1 as (select ORDER_ID, PRICE > ? from test_kylin_fact where ORDER_ID > ?)\n, "
                + "t2 as (select bf from test_kylin_fact)\n" + "select * from t1 where ORDER_ID = '125'\n" //
                + "union all\n" //
                + "select * from t1 where ORDER_ID < '200'";
        String filledSql1 = "with t1 as (select ORDER_ID, PRICE > 123.1 from test_kylin_fact where ORDER_ID > 123)\n"
                + ", t2 as (select bf from test_kylin_fact)\n" + "select * from t1 where ORDER_ID = '125'\n"
                + "union all\n" + "select * from t1 where ORDER_ID < '200'";
        String transformedFilledSql1 = "SELECT *\n" + "FROM (SELECT ORDER_ID, PRICE > 123.1\n"
                + "FROM TEST_KYLIN_FACT\n" + "WHERE ORDER_ID > 123) AS T1\n" + "WHERE ORDER_ID = '125'\n"
                + "UNION ALL\n" + "SELECT *\n" + "FROM (SELECT ORDER_ID, PRICE > 123.1\n" + "FROM TEST_KYLIN_FACT\n"
                + "WHERE ORDER_ID > 123) AS T1\n" + "WHERE ORDER_ID < '200'";
        queryWithParamWhenTransformWithToSubQuery(params1, originSql1, filledSql1, transformedFilledSql1);

        PrepareSqlStateParam[] params2 = new PrepareSqlStateParam[1];
        params2[0] = new PrepareSqlStateParam(Double.class.getCanonicalName(), "456.1");
        String originSql2 = "with t1 as\n"
                + " (select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) as sum_price, PRICE > ?\n"
                + " from test_kylin_fact\n" + "inner JOIN edw.test_cal_dt as test_cal_dt\n"
                + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" + " inner JOIN test_category_groupings\n"
                + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + " inner JOIN edw.test_sites as test_sites\n"
                + " ON test_kylin_fact.lstg_site_id = test_sites.site_id\n"
                + " group by test_cal_dt.week_beg_dt, PRICE)\n" + "\n" + "SELECT sum(sum_price) AS \"COL\"\n"
                + " FROM t1 HAVING COUNT(1)>0 limit 10";
        String filledSql2 = "with t1 as\n"
                + " (select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) as sum_price, PRICE > 456.1\n"
                + " from test_kylin_fact\n" + "inner JOIN edw.test_cal_dt as test_cal_dt\n"
                + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" + " inner JOIN test_category_groupings\n"
                + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + " inner JOIN edw.test_sites as test_sites\n"
                + " ON test_kylin_fact.lstg_site_id = test_sites.site_id\n"
                + " group by test_cal_dt.week_beg_dt, PRICE)\n" + "\n" + "SELECT sum(sum_price) AS \"COL\"\n"
                + " FROM t1 HAVING COUNT(1)>0 limit 10";
        String transformedFilledSql2 = "SELECT SUM(SUM_PRICE) AS COL\n"
                + "FROM (SELECT TEST_CAL_DT.WEEK_BEG_DT, SUM(TEST_KYLIN_FACT.PRICE) AS SUM_PRICE, PRICE > 456.1\n"
                + "FROM TEST_KYLIN_FACT\n"
                + "INNER JOIN EDW.TEST_CAL_DT AS TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "INNER JOIN TEST_CATEGORY_GROUPINGS ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES AS TEST_SITES ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "GROUP BY TEST_CAL_DT.WEEK_BEG_DT, PRICE) AS T1\n" + "HAVING COUNT(1) > 0";
        queryWithParamWhenTransformWithToSubQuery(params2, originSql2, filledSql2, transformedFilledSql2);

        String originSql3 = "with t1 as (select * from test_kylin_fact),\n"
                + "\t t2 as (select ORDER_ID, PRICE, CAL_DT from test_kylin_fact union all select ORDER_ID, PRICE, CAL_DT from t1),\n"
                + "\t t3 as (select sum(PRICE) as sum_price, order_id, CAL_DT from t2 group by order_id, CAL_DT order by order_id)\n"
                + "select * from t3 limit 10";
        String transformedFilledSql3 = "SELECT *\n" + "FROM (SELECT SUM(PRICE) AS SUM_PRICE, ORDER_ID, CAL_DT\n"
                + "FROM (SELECT ORDER_ID, PRICE, CAL_DT\n" + "FROM TEST_KYLIN_FACT\n" + "UNION ALL\n"
                + "SELECT ORDER_ID, PRICE, CAL_DT\n" + "FROM (SELECT *\n" + "FROM TEST_KYLIN_FACT) AS T1) AS T2\n"
                + "GROUP BY ORDER_ID, CAL_DT\n" + "ORDER BY ORDER_ID) AS T3\n" + "FETCH NEXT 10 ROWS ONLY";
        queryWithParamWhenTransformWithToSubQuery(null, originSql3, originSql3, transformedFilledSql3);

        String originSql4 = "with t1 as (select TEST_KYLIN_FACT.DEAL_AMOUNT from TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ORDER as TEST_ORDER\n" + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n"
                + "LEFT JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n"
                + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n"
                + "LEFT JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n"
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "LEFT JOIN EDW.TEST_SITES as TEST_SITES\n" + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n"
                + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "LEFT JOIN TEST_COUNTRY as SELLER_COUNTRY\n"
                + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY\n"
                + "LEFT JOIN TEST_COUNTRY as BUYER_COUNTRY\n"
                + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY),\n"
                + "\t t2 as (select * from t1 union all select * from t1)\n" + "select * from t2 limit 10";
        String transformedFilledSql4 = "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT TEST_KYLIN_FACT.DEAL_AMOUNT\n"
                + "FROM TEST_KYLIN_FACT AS TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ORDER AS TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n"
                + "LEFT JOIN EDW.TEST_SELLER_TYPE_DIM AS TEST_SELLER_TYPE_DIM ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n"
                + "LEFT JOIN EDW.TEST_CAL_DT AS TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_CATEGORY_GROUPINGS AS TEST_CATEGORY_GROUPINGS ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "LEFT JOIN EDW.TEST_SITES AS TEST_SITES ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "LEFT JOIN TEST_ACCOUNT AS SELLER_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n"
                + "LEFT JOIN TEST_ACCOUNT AS BUYER_ACCOUNT ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "LEFT JOIN TEST_COUNTRY AS SELLER_COUNTRY ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY\n"
                + "LEFT JOIN TEST_COUNTRY AS BUYER_COUNTRY ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY) AS T1\n"
                + "UNION ALL\n" + "SELECT *\n" + "FROM (SELECT TEST_KYLIN_FACT.DEAL_AMOUNT\n"
                + "FROM TEST_KYLIN_FACT AS TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ORDER AS TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n"
                + "LEFT JOIN EDW.TEST_SELLER_TYPE_DIM AS TEST_SELLER_TYPE_DIM ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n"
                + "LEFT JOIN EDW.TEST_CAL_DT AS TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_CATEGORY_GROUPINGS AS TEST_CATEGORY_GROUPINGS ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "LEFT JOIN EDW.TEST_SITES AS TEST_SITES ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "LEFT JOIN TEST_ACCOUNT AS SELLER_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n"
                + "LEFT JOIN TEST_ACCOUNT AS BUYER_ACCOUNT ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "LEFT JOIN TEST_COUNTRY AS SELLER_COUNTRY ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY\n"
                + "LEFT JOIN TEST_COUNTRY AS BUYER_COUNTRY ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY) AS T1) AS T2\n"
                + "FETCH NEXT 10 ROWS ONLY";
        queryWithParamWhenTransformWithToSubQuery(null, originSql4, originSql4, transformedFilledSql4);
    }

    private void queryWithParamWhenTransformWithToSubQuery(PrepareSqlStateParam[] params, String originSql,
            String filledSql, String transformedFilledSql) {
        // prepare query request
        final PrepareSqlRequest request = new PrepareSqlRequest();
        request.setProject("default");
        request.setSql(originSql);
        request.setParams(params);
        final QueryContext queryContext = QueryContext.current();
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);

        // 1. validate transform with to sub query when replace-dynamic-params open
        overwriteSystemProp("kylin.query.replace-dynamic-params-enabled", "true");
        queryService.doQueryWithCache(request);
        Assert.assertEquals(queryContext.getUserSQL(), originSql);
        // sql after massage will transformed and filled params
        Assert.assertTrue(queryContext.getMetrics().getOlapCause().getMessage().contains(transformedFilledSql));
        // validate sql Metrics for query history(pushdown). sql reset to filledSql after pushdown(on tryPushDownSelectQuery).
        QueryMetricsContext.start(queryContext.getQueryId(), "");
        Assert.assertTrue(QueryMetricsContext.isStarted());
        if (params != null) {
            Assert.assertEquals(filledSql, queryContext.getMetrics().getCorrectedSql());
            Assert.assertEquals(filledSql, QueryMetricsContext.collect(queryContext).getSql());
        } else {
            Assert.assertEquals(transformedFilledSql, queryContext.getMetrics().getCorrectedSql());
            Assert.assertEquals(transformedFilledSql, QueryMetricsContext.collect(queryContext).getSql());
        }
        QueryMetricsContext.reset();
        // validate sql Metrics for query history(model)
        queryContext.getMetrics().setCorrectedSql(transformedFilledSql);
        QueryMetricsContext.start(queryContext.getQueryId(), "");
        Assert.assertTrue(QueryMetricsContext.isStarted());
        Assert.assertEquals(transformedFilledSql, QueryMetricsContext.collect(queryContext).getSql());
        QueryMetricsContext.reset();

        // 2. validate transform with to sub query when replace-dynamic-params close
        overwriteSystemProp("kylin.query.replace-dynamic-params-enabled", "false");
        queryService.doQueryWithCache(request);
        Assert.assertEquals(queryContext.getUserSQL(), originSql);
        // when replace-dynamic-params close, transform with to subQuery close too
        Assert.assertTrue(queryContext.getMetrics().getOlapCause().getMessage().contains(originSql));
        // validate sql Metrics for query history(pushdown)
        Assert.assertEquals(filledSql, queryContext.getMetrics().getCorrectedSql());
        QueryMetricsContext.start(queryContext.getQueryId(), "");
        Assert.assertTrue(QueryMetricsContext.isStarted());
        Assert.assertEquals(filledSql, QueryMetricsContext.collect(queryContext).getSql());
        QueryMetricsContext.reset();
    }

    private void updateProjectConfig(String project, String property, String value) {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(project,
                copyForWrite -> copyForWrite.getOverrideKylinProps().put(property, value));
    }

    @Test
    public void testQueryWhenHitBlacklist() throws Exception {
        overwriteSystemProp("kylin.query.blacklist-enabled", "true");
        final String project = "default";
        final String sql = "select count(*) from test_kylin_fact";

        stubQueryConnection(sql, project);
        mockOLAPContext();

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        QueryService spiedQueryService = Mockito.spy(queryService);
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(spiedQueryService);
        SQLResponse sqlResponse = spiedQueryService.doQueryWithCache(request);
        Assert.assertFalse(sqlResponse.isException());

        SQLBlacklistManager sqlBlacklistManager = SQLBlacklistManager.getInstance(KylinConfig.getInstanceFromEnv());
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setId("1");
        sqlBlacklistItem.setSql("select count(*) from test_kylin_fact");
        sqlBlacklistManager.addSqlBlacklistItem(project, sqlBlacklistItem);

        sqlResponse = spiedQueryService.doQueryWithCache(request);
        Assert.assertTrue(sqlResponse.isException());
        Assert.assertEquals("Query is rejected by blacklist, blacklist item id: 1.", sqlResponse.getExceptionMessage());
        queryCacheManager.clearQueryCache(request);
    }

    @Test
    public void testQueryWhenHitBlacklistConcurrent() throws Exception {
        overwriteSystemProp("kylin.query.blacklist-enabled", "true");
        overwriteSystemProp("kylin.query.cache-enabled", "false");
        final String project = "default";
        final String sql = "select count(*) from test_kylin_fact";

        stubQueryConnection(sql, project);
        mockOLAPContext();

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        SQLResponse sqlResponse = queryService.doQueryWithCache(request);
        Assert.assertFalse(sqlResponse.isException());

        SQLBlacklistManager sqlBlacklistManager = SQLBlacklistManager.getInstance(KylinConfig.getInstanceFromEnv());
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setId("1");
        sqlBlacklistItem.setConcurrentLimit(1);
        sqlBlacklistItem.setSql("select count(*) from test_kylin_fact");
        sqlBlacklistManager.addSqlBlacklistItem(project, sqlBlacklistItem);

        SQLResponse resp = queryService.doQueryWithCache(request);
        queryService.slowQueryDetector.queryStart("1"); // pretending that the query is started
        Assert.assertFalse(resp.isException());

        SQLResponse resp1 = queryService.doQueryWithCache(request);
        Assert.assertTrue(resp1.isException());
        Assert.assertEquals(
                String.format(Locale.ROOT, MsgPicker.getMsg().getSqlBlackListQueryConcurrentLimitExceeded(), "1", 1),
                resp1.getExceptionMessage());
    }

    @Test
    public void testExceptionCache() throws Exception {
        overwriteSystemProp("kylin.query.cache-enabled", "false");
        overwriteSystemProp("kylin.query.exception-cache-enabled", "true");
        overwriteSystemProp("kylin.query.exception-cache-threshold-times", "2");
        overwriteSystemProp("kylin.query.exception-cache-threshold-duration", "100");

        final String project = "default";
        final String sql = "select count(*) from test_kylin_fact";

        stubQueryConnection(sql, project);
        mockOLAPContext();

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        Mockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        SQLResponse sqlResponse = queryService.doQueryWithCache(request);
        sqlResponse.setException(true);
        sqlResponse.setExceptionMessage("error");

        overwriteSystemProp("kylin.query.cache-enabled", "true");
        sqlResponse.setDuration(200);
        queryService.putIntoExceptionCache(request, sqlResponse, new RuntimeException("foo"));
        val ret1 = queryService.doQueryWithCache(request);
        Assert.assertFalse(ret1.isException());

        sqlResponse.setDuration(200);
        queryService.putIntoExceptionCache(request, sqlResponse, new RuntimeException("foo"));
        sqlResponse.setDuration(200);
        queryService.putIntoExceptionCache(request, sqlResponse, new RuntimeException("foo"));
        val ret2 = queryService.doQueryWithCache(request);
        Assert.assertTrue(ret2.isException());
    }

    @Test
    public void testGetForcedToTieredStorageValueInvalid() {
        try {
            ForceToTieredStorage f = ForceToTieredStorage.values()[-1];
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ArrayIndexOutOfBoundsException);
        }
        ForceToTieredStorage f = ForceToTieredStorage.values()[0];
        assert ForceToTieredStorage.CH_FAIL_TO_DFS == f;
        f = ForceToTieredStorage.values()[1];
        assert ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN == f;
        f = ForceToTieredStorage.values()[2];
        assert ForceToTieredStorage.CH_FAIL_TO_RETURN == f;
        f = ForceToTieredStorage.values()[3];
        assert ForceToTieredStorage.CH_FAIL_TAIL == f;
    }

    @Test
    public void testGetForcedToTieredStorageForProject() {
        for (ForceToTieredStorage j : ForceToTieredStorage.values()) {
            String project = Integer.toString(j.ordinal());
            overwriteSystemProp("kylin.second-storage.route-when-ch-fail", project);
            ForceToTieredStorage ret = queryService.getForcedToTieredStorage("default", j);
            if (j != ForceToTieredStorage.CH_FAIL_TAIL) {
                assert j == ret;
            } else {
                assert ForceToTieredStorage.CH_FAIL_TO_DFS == ret;
            }
        }
    }

    @Test
    public void testGetForcedToTieredStorageForSystem() {
        for (ForceToTieredStorage j : ForceToTieredStorage.values()) {
            String project = Integer.toString(j.ordinal());
            overwriteSystemProp("kylin.second-storage.route-when-ch-fail", project);
            ForceToTieredStorage ret = queryService.getForcedToTieredStorage("default", j);
            if (j != ForceToTieredStorage.CH_FAIL_TAIL) {
                assert j == ret;
            } else {
                assert ForceToTieredStorage.CH_FAIL_TO_DFS == ret;
            }
        }
    }

    @Test
    public void testGetForcedToTieredStorageForMismatch() {
        overwriteSystemProp("kylin.second-storage.route-when-ch-fail", "1");
        ForceToTieredStorage ret = queryService.getForcedToTieredStorage("default", ForceToTieredStorage.CH_FAIL_TAIL);
        assert ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN == ret;
        ret = queryService.getForcedToTieredStorage("default", ForceToTieredStorage.CH_FAIL_TO_DFS);
        assert ForceToTieredStorage.CH_FAIL_TO_DFS == ret;
    }

    @Test
    public void testForceToTieredStorageOK() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToTieredStorage(0);

        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                queryExec.getDefaultSchemaName(), true);
        queryParams.setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_DFS);
        String correctedSql = QueryUtil.massageSql(queryParams);

        Mockito.when(queryExec.executeQuery(correctedSql)).thenReturn(new QueryResult());
        Mockito.doReturn(new QueryResult()).when(queryService.queryRoutingEngine).execute(Mockito.any(), Mockito.any());

        final SQLResponse response = queryService.queryWithCache(sqlRequest);
        Assert.assertNull(response.getExceptionMessage());
    }

    @Test
    public void testForceToTieredStoragePushDown() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToTieredStorage(1);

        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                queryExec.getDefaultSchemaName(), true);
        String correctedSql = QueryUtil.massageSql(queryParams);

        Throwable cause = new SQLException(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE);
        Mockito.doThrow(
                new SQLException("Error while executing SQL \"" + correctedSql + "\": " + cause.getMessage(), cause))
                .when(queryService.queryRoutingEngine).execute(Mockito.any(), Mockito.any());

        Mockito.doAnswer(invocation -> PushdownResult.emptyResult()).when(queryService.queryRoutingEngine)
                .tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        final SQLResponse response = queryService.queryWithCache(sqlRequest);
        Assert.assertTrue(response.isQueryPushDown());
    }

    @Test
    public void testForceToTieredStorageInvalidParameters() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToTieredStorage(1);
        sqlRequest.setForcedToIndex(true);
        sqlRequest.setForcedToPushDown(false);

        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                queryExec.getDefaultSchemaName(), true);
        String correctedSql = QueryUtil.massageSql(queryParams);

        Throwable cause = new KylinException(QueryErrorCode.FORCED_TO_TIEREDSTORAGE_AND_FORCE_TO_INDEX,
                MsgPicker.getMsg().getForcedToTieredstorageAndForceToIndex());
        Mockito.doThrow(
                new SQLException("Error while executing SQL \"" + correctedSql + "\": " + cause.getMessage(), cause))
                .when(queryService.queryRoutingEngine).execute(Mockito.any(), Mockito.any());

        final SQLResponse response = queryService.queryWithCache(sqlRequest);
        Assert.assertTrue(
                response.getExceptionMessage().contains(MsgPicker.getMsg().getForcedToTieredstorageAndForceToIndex()));
    }

    @Test
    public void testForceToTieredStorageReturnError() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = PowerMockito.mock(QueryExec.class);
        PowerMockito.whenNew(QueryExec.class).withAnyArguments().thenReturn(queryExec);

        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToTieredStorage(2);

        QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                "queryExec.getDefaultSchemaName()", true);
        String correctedSql = QueryUtil.massageSql(queryParams);

        Throwable cause = new KylinException(QueryErrorCode.FORCED_TO_TIEREDSTORAGE_RETURN_ERROR,
                MsgPicker.getMsg().getForcedToTieredstorageReturnError());
        Mockito.doThrow(
                new SQLException("Error while executing SQL \"" + correctedSql + "\": " + cause.getMessage(), cause))
                .when(queryService.queryRoutingEngine).execute(Mockito.any(), Mockito.any());

        final SQLResponse response = queryService.queryWithCache(sqlRequest);
        Assert.assertTrue(
                response.getExceptionMessage().contains(MsgPicker.getMsg().getForcedToTieredstorageReturnError()));
    }

    @Test
    public void testCheckSqlRequestProject() {
        SQLRequest sqlRequest = new SQLRequest();
        Message msg = MsgPicker.getMsg();
        Assert.assertThrows(KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(queryService, "checkSqlRequestProject", sqlRequest, msg));
    }

    @Test
    public void testGetMetadataWithModelName() throws Exception {
        String project = "default";
        String cube = "nmodel_basic_inner";
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "true");
        List<TableMeta> tableMetas = queryService.getMetadata(project, cube);
        List<ColumnMeta> columnMetas = tableMetas.stream()
                .filter(tableMeta -> tableMeta.getTABLE_NAME().equals("TEST_KYLIN_FACT")).findFirst().get()
                .getColumns();
        Assert.assertEquals(12, columnMetas.size());
        Assert.assertFalse(columnMetas.stream()
                .anyMatch(columnMeta -> columnMeta.getCOLUMN_NAME().equals("LEFTJOIN_SELLER_COUNTRY_ABBR")));
    }

    public void setCalendarMock() {
        Calendar mockCalendar = Calendar.getInstance();
        mockCalendar.setTimeInMillis(1622432018000L);
        List<Calendar> calendarSet = new ArrayList<>();
        while (calendarSet.size() < 20) {
            calendarSet.add((Calendar) mockCalendar.clone());
        }
        PowerMockito.mockStatic(Calendar.class);
        PowerMockito.when(Calendar.getInstance()).thenReturn(mockCalendar,
                calendarSet.toArray(new Calendar[calendarSet.size()]));
    }

    @Test
    public void testDateNumberFilterTransformer() {
        final DateNumberFilterTransformer transformer = new DateNumberFilterTransformer();
        setCalendarMock();
        String originSql1 = "select count(1) from KYLIN_SALES where \n" + "    {fn YEAR(PART_DT)} = 2012\n"
                + "and {fn YEAR(PART_DT)} <> 2011\n" + "and {fn YEAR(PART_DT)} > 2011\n"
                + "and {fn YEAR(PART_DT)} >= 2012\n" + "and {fn YEAR(PART_DT)} < 2013\n"
                + "and {fn YEAR(PART_DT)} <= 2012\n" + "and {fn YEAR(PART_DT)} between 2011 AND 2013\n"
                + "and {fn YEAR(PART_DT)} not between 2013 AND 2014\n" + "and {fn YEAR(PART_DT)} not in (2013, 2015)\n"
                + "and YEAR(PART_DT) in (2012, 2011)\n"
                + "and 201201 = {fn YEAR(PART_DT)} * 100 + {fn MONTH(PART_DT)}\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn MONTH(PART_DT)} <> 201205\n"
                + "and 100 * {fn YEAR(PART_DT)} + {fn MONTH(PART_DT)} > 201112\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn MONTH(PART_DT)} >= 201112\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn MONTH(PART_DT)} < 201305\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn MONTH(PART_DT)} <= 201205\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn MONTH(PART_DT)} between 201101 AND 201305\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn MONTH(PART_DT)} not between 201205 AND 201307\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn MONTH(PART_DT)} not in (201305, 201306)\n"
                + "and YEAR(PART_DT) * 100 + MONTH(PART_DT) in (201201, 201202)\n"
                + "and {fn YEAR(PART_DT)} * 10000 + {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)} = 20120101\n"
                + "and {fn YEAR(PART_DT)} * 10000 + {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)} <> 20120501\n"
                + "and {fn YEAR(PART_DT)} * 10000 + {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)} > 20111231\n"
                + "and {fn YEAR(PART_DT)} * 10000 + {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)} >= 20111231\n"
                + "and {fn YEAR(PART_DT)} * 10000 + {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)} < 20130501\n"
                + "and {fn YEAR(PART_DT)} * 10000 + {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)} <= 20120501\n"
                + "and {fn YEAR(PART_DT)} * 10000 + {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)} between 20110131 AND 20130501\n"
                + "and {fn year(PART_DT)} * 10000 + {fn month(PART_DT)} * 100 + {fn dayofmonth(PART_DT)} not between 20120501 AND 20130702\n"
                + "and {fn YEAR(PART_DT)} * 10000 + DAYOFMONTH(PART_DT) + {fn MONTH(PART_DT)} * 100 not in (20130501, 20130631)\n"
                + "and YEAR(PART_DT) * 10000 + MONTH(PART_DT) * 100 + DAYOFMONTH(PART_DT) in (20120101, 20120201)";
        String expectedSql1 = "select count(1) from KYLIN_SALES where \n"
                + "    cast(\"PART_DT\" as date) BETWEEN '2012-01-01' and '2012-12-31'\n"
                + "and cast(\"PART_DT\" as date) NOT BETWEEN '2011-01-01' and '2011-12-31'\n"
                + "and cast(\"PART_DT\" as date) > '2011-12-31'\n" + "and cast(\"PART_DT\" as date) >= '2012-01-01'\n"
                + "and cast(\"PART_DT\" as date) < '2013-01-01'\n" + "and cast(\"PART_DT\" as date) <= '2012-12-31'\n"
                + "and cast(\"PART_DT\" as date) BETWEEN '2011-01-01' and '2013-12-31'\n"
                + "and cast(\"PART_DT\" as date) NOT BETWEEN '2013-01-01' and '2014-12-31'\n"
                + "and (cast(\"PART_DT\" as date) NOT BETWEEN '2013-01-01' and '2013-12-31' AND cast(\"PART_DT\" as date) NOT BETWEEN '2015-01-01' and '2015-12-31')\n"
                + "and (cast(\"PART_DT\" as date) BETWEEN '2012-01-01' and '2012-12-31' OR cast(\"PART_DT\" as date) BETWEEN '2011-01-01' and '2011-12-31')\n"
                + "and cast(\"PART_DT\" as date) BETWEEN '2012-01-01' and '2012-01-31'\n"
                + "and cast(\"PART_DT\" as date) NOT BETWEEN '2012-05-01' and '2012-05-31'\n"
                + "and cast(\"PART_DT\" as date) > '2011-12-31'\n" + "and cast(\"PART_DT\" as date) >= '2011-12-01'\n"
                + "and cast(\"PART_DT\" as date) < '2013-05-01'\n" + "and cast(\"PART_DT\" as date) <= '2012-05-31'\n"
                + "and cast(\"PART_DT\" as date) BETWEEN '2011-01-01' and '2013-05-31'\n"
                + "and cast(\"PART_DT\" as date) NOT BETWEEN '2012-05-01' and '2013-07-31'\n"
                + "and (cast(\"PART_DT\" as date) NOT BETWEEN '2013-05-01' and '2013-05-31' AND cast(\"PART_DT\" as date) NOT BETWEEN '2013-06-01' and '2013-06-30')\n"
                + "and (cast(\"PART_DT\" as date) BETWEEN '2012-01-01' and '2012-01-31' OR cast(\"PART_DT\" as date) BETWEEN '2012-02-01' and '2012-02-29')\n"
                + "and cast(\"PART_DT\" as date) = '2012-01-01'\n" + "and cast(\"PART_DT\" as date) <> '2012-05-01'\n"
                + "and cast(\"PART_DT\" as date) > '2011-12-31'\n" + "and cast(\"PART_DT\" as date) >= '2011-12-31'\n"
                + "and cast(\"PART_DT\" as date) < '2013-05-01'\n" + "and cast(\"PART_DT\" as date) <= '2012-05-01'\n"
                + "and cast(\"PART_DT\" as date) BETWEEN '2011-01-31' and '2013-05-01'\n"
                + "and cast(\"PART_DT\" as date) NOT BETWEEN '2012-05-01' and '2013-07-02'\n"
                + "and (cast(\"PART_DT\" as date) <> '2013-05-01' AND cast(\"PART_DT\" as date) <> '2013-06-31')\n"
                + "and (cast(\"PART_DT\" as date) = '2012-01-01' OR cast(\"PART_DT\" as date) = '2012-02-01')";
        String transformedSql1 = transformer.transform(originSql1, null, null);
        Assert.assertEquals(expectedSql1, transformedSql1);

        String originSql2 = "select count(1) from KYLIN_SALES where {fn YEAR(PART_DT)} in (2012,2013,100) and year(PART_DT) in (2012,2009)";
        String expectedSql2 = "select count(1) from KYLIN_SALES where {fn YEAR(PART_DT)} in (2012,2013,100) and (cast(\"PART_DT\" as date) "
                + "BETWEEN '2012-01-01' and '2012-12-31' OR cast(\"PART_DT\" as date) BETWEEN '2009-01-01' and '2009-12-31')";
        String transformedSql2 = transformer.transform(originSql2, null, null);
        Assert.assertEquals(expectedSql2, transformedSql2);

        String originSql3 = "select count(1) from KYLIN_SALES where ((({fn YEAR(\"kylin\".\"PART_DT\")} * 10000) + {fn MONTH(\"kylin\".\"PART_DT\")} * 100) + {fn DAYOFMONTH(\"kylin\".\"PART_DT\")}) = 20120101";
        String expectedSql3 = "select count(1) from KYLIN_SALES where cast(\"kylin\".\"PART_DT\" as date) = '2012-01-01'";
        String transformedSql3 = transformer.transform(originSql3, null, null);
        Assert.assertEquals(expectedSql3, transformedSql3);

        String originSql4 = "select count(1) from KYLIN_SALES where {fn YEAR(cast(PART_DT as date))} = 2012";
        String expectedSql4 = "select count(1) from KYLIN_SALES where cast(CAST(\"PART_DT\" AS DATE) as date) BETWEEN '2012-01-01' and '2012-12-31'";
        String transformedSql4 = transformer.transform(originSql4, null, null);
        Assert.assertEquals(expectedSql4, transformedSql4);

        String originSql5 = "select count(1) from KYLIN_SALES where"
                + "{fn YEAR(case when PART_DT = '1990-01-02' then PART_DT else cast(PART_DT as date) end)} * 10000 + {fn month(PART_DT)} * 100 + {fn dayofmonth(PART_DT)} != 20120203\n"
                + "and {fn YEAR(cast(PART_DT as date))} * 100 + {fn month(PART_DT)} = '201202'\n"
                + "and {fn YEAR(PART_DT)} * 10 + {fn month(PART_DT)} != 201202\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn month(PART_DT)} * 10 != 201202\n"
                + "and {fn YEAR(PART_DT)} * 1000 + {fn month(PART_DT)} * 100 != 201202\n"
                + "and {fn YEAR(PART_DT)} * 1000 + {fn month(PART_DT)} * '100' != 201202\n"
                + "and {fn YEAR(PART_DT)} * 100 + {fn month(PART_DT)} != 20120203\n"
                + "and {fn YEAR(PART_DT)} / 100 + {fn month(PART_DT)} != 20120203\n"
                + "and {fn YEAR(PART_DT)} * '100' + {fn month(PART_DT)} != 20120203\n"
                + "and {fn YEAR(PART_DT)} + 1000 + {fn month(PART_DT)} + 100 != 201202\n"
                + "and {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)}  != 201202\n"
                + "and {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)}  != 1202\n"
                + "and 1000 + {fn YEAR(PART_DT)} + {fn month(PART_DT)} * 100 != 201202\n"
                + "and 1000 + {fn YEAR(PART_DT)} + {fn month(PART_DT)} * 100 != 201202\n"
                + "and {fn YEAR(cast(PART_DT as date))} * 100 + {fn month(PART_DT)} * 10 != 201202\n"
                + "and {fn YEAR(PART_DT)} * 100000 + {fn MONTH(PART_DT)} * 1001 + {fn DAYOFMONTH(PART_DT)} * 1 != 20120201\n"
                + "and {fn YEAR(PART_DT)} / 1000 + {fn MONTH(PART_DT)} * 1000 + {fn DAYOFMONTH(PART_DT)} * 1 != 20120201\n"
                + "and {fn YEAR(PART_DT)} * 100000 + {fn MONTH(PART_DT)} * '100' + {fn DAYOFMONTH(PART_DT)} * 1 != '20120201'\n"
                + "and {fn YEAR(PART_DT)} * 1000 + {fn MONTH(PART_DT)} * 100 + {fn DAYOFMONTH(PART_DT)}  != '20120201'";
        String transformedSql5 = transformer.transform(originSql5, null, null);
        Assert.assertEquals(originSql5, transformedSql5);
    }

    @Test
    public void testMetaDataReturnOnlyIndexPlanColsAndJoinKey() throws Exception {
        String project = "default";
        String modelName = "nmodel_basic_inner";
        overwriteSystemProp("kylin.model.tds-expose-all-model-related-columns", "false");
        List<TableMeta> tableMetas = queryService.getMetadata(project, modelName);
        List<ColumnMeta> columnMetas = tableMetas.stream()
                .filter(tableMeta -> tableMeta.getTABLE_NAME().equals("TEST_ACCOUNT")).findFirst().get().getColumns();
        Assert.assertEquals(8, tableMetas.size());
        Assert.assertEquals(4, columnMetas.size());
    }

    @Test
    public void testMetadataReturnOnlyIndexPlanCols() throws Exception {
        String project = "default";
        String modelName = "nmodel_basic_inner";
        overwriteSystemProp("kylin.model.tds-expose-all-model-related-columns", "false");
        overwriteSystemProp("kylin.model.tds-expose-model-join-key", "false");
        List<TableMeta> tableMetas = queryService.getMetadata(project, modelName);
        List<ColumnMeta> columnMetas = tableMetas.stream()
                .filter(tableMeta -> tableMeta.getTABLE_NAME().equals("TEST_ACCOUNT")).findFirst().get().getColumns();
        Assert.assertEquals(3, columnMetas.size());
    }

    @Test
    public void testGetTargetModelColumns() {
        String project = "default";
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<NDataModel> dataModels = dataModelManager.listAllModels();
        overwriteSystemProp("kylin.model.tds-expose-all-model-related-columns", "true");
        List<String> modelColumns1 = queryService.getTargetModelColumns("nmodel_basic", dataModels, project);
        Assert.assertEquals(861, modelColumns1.size());
        overwriteSystemProp("kylin.model.tds-expose-all-model-related-columns", "false");
        List<String> modelColumns2 = queryService.getTargetModelColumns("nmodel_basic", dataModels, project);
        Assert.assertEquals(172, modelColumns2.size());
    }

    @Test
    public void testDistinctAggregationInSql() throws Exception {
        final String project = "default";
        String sql1 = "SELECT COUNT(DISTINCT TEST_BANK_INCOME.INCOME) FROM TEST_BANK_INCOME inner join TEST_BANK_LOCATION on TEST_BANK_INCOME.COUNTRY = TEST_BANK_LOCATION.COUNTRY WHERE \n"
                + "1 = 1\n" + "and TEST_BANK_INCOME.DT = '2021-11-02'\n"
                + "and TEST_BANK_INCOME.COUNTRY = 'INDONESIA'\n" + "and TEST_BANK_INCOME.COUNTRY = 'KENYA'";
        QueryExec queryExec = new QueryExec(project, getTestConfig());
        Class<? extends QueryExec> clazz = queryExec.getClass();
        Method isCalciteEngineCapable = clazz.getDeclaredMethod("isCalciteEngineCapable", RelNode.class);
        isCalciteEngineCapable.setAccessible(true);
        RelNode rel1 = queryExec.parseAndOptimize(sql1);
        QueryResult queryResult1 = queryExec.executeQuery(sql1);
        Assert.assertEquals(1, queryResult1.getColumns().size());
        Object routeToCalcite1 = isCalciteEngineCapable.invoke(queryExec, rel1);
        Assert.assertEquals(false, routeToCalcite1);

        String sql2 = "SELECT COUNT(*) FROM TEST_BANK_INCOME inner join TEST_BANK_LOCATION on TEST_BANK_INCOME.COUNTRY = TEST_BANK_LOCATION.COUNTRY WHERE \n"
                + "1 = 1\n" + "and TEST_BANK_INCOME.DT = '2021-11-02'\n"
                + "and TEST_BANK_INCOME.COUNTRY = 'INDONESIA'\n" + "and TEST_BANK_INCOME.COUNTRY = 'KENYA'";
        RelNode rel2 = queryExec.parseAndOptimize(sql2);
        Object routeToCalcite2 = isCalciteEngineCapable.invoke(queryExec, rel2);
        Assert.assertEquals(true, routeToCalcite2);
    }

    @Test
    public void testStop() {
        val stopId = RandomUtil.randomUUIDStr();
        val execute = new Thread(() -> {
            QueryContext.current().setQueryId(RandomUtil.randomUUIDStr());
            QueryContext.current().setProject("default");
            QueryContext.current().setUserSQL("select 1");
            queryService.slowQueryDetector.queryStart(stopId);
            await().pollDelay(new Duration(5, SECONDS)).until(() -> true);
        });
        execute.start();
        await().pollDelay(new Duration(1, SECONDS)).until(() -> true);
        queryService.stopQuery(stopId);
        val result = SlowQueryDetector.getRunningQueries().values().stream()
                .filter(entry -> StringUtils.equals(stopId, entry.getStopId())).findFirst();
        Assert.assertTrue(result.isPresent());
        val queryEntry = result.get();
        Assert.assertTrue(queryEntry.getPlannerCancelFlag().isCancelRequested());
    }
}
