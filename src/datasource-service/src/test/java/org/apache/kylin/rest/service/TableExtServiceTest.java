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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.S3TableExtInfo;
import org.apache.kylin.rest.request.UpdateAWSTableExtDescRequest;
import org.apache.kylin.rest.response.LoadTableResponse;
import org.apache.kylin.rest.response.UpdateAWSTableExtDescResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TableExtServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private final TableService tableService = Mockito.spy(TableService.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private final TableExtService tableExtService = Mockito.spy(new TableExtService());

    @Before
    public void setup() throws IOException {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableExtService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableExtService, "tableService", tableService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testLoadTables() throws Exception {
        String[] tables = { "DEFAULT.TEST_KYLIN_FACT", "DEFAULT.TEST_ACCOUNT" };
        String[] tableNames = { "TEST_KYLIN_FACT", "TEST_ACCOUNT" };
        List<Pair<TableDesc, TableExtDesc>> result = mockTablePair(2, "DEFAULT");
        Mockito.doReturn(result).when(tableService).extractTableMeta(Mockito.any(), Mockito.any());
        Mockito.doNothing().when(tableExtService).loadTable(result.get(0).getFirst(), result.get(0).getSecond(),
                "default");
        Mockito.doNothing().when(tableExtService).loadTable(result.get(1).getFirst(), result.get(1).getSecond(),
                "default");
        Mockito.doReturn(Lists.newArrayList(tableNames)).when(tableService).getSourceTableNames("default", "DEFAULT",
                "");
        Mockito.doReturn(Lists.newArrayList("DEFAULT")).when(tableService).getSourceDbNames("default");

        LoadTableResponse response = tableExtService.loadDbTables(tables, "default", false);
        Assert.assertEquals(2, response.getLoaded().size());
    }

    @Test
    public void testLoadAWSTablesCompatibleCrossAccount() throws Exception {
        String[] tableNames = { "TABLE0", "TABLE1" };
        List<S3TableExtInfo> crossAccountTableReq = new ArrayList<>();
        S3TableExtInfo s3TableExtInfo1 = new S3TableExtInfo();
        s3TableExtInfo1.setName("DEFAULT.TABLE0");
        s3TableExtInfo1.setLocation("s3://bucket1/test1/");
        S3TableExtInfo s3TableExtInfo2 = new S3TableExtInfo();
        s3TableExtInfo2.setName("DEFAULT.TABLE1");
        s3TableExtInfo2.setLocation("s3://bucket2/test2/");
        s3TableExtInfo2.setEndpoint("us-west-2.amazonaws.com");
        s3TableExtInfo2.setRoleArn("test:role");
        crossAccountTableReq.add(s3TableExtInfo1);
        crossAccountTableReq.add(s3TableExtInfo2);
        List<Pair<TableDesc, TableExtDesc>> result = mockTablePair(2, "DEFAULT", "TABLE");
        Mockito.doReturn(result).when(tableService).extractTableMeta(Mockito.any(), Mockito.any());
        Mockito.doNothing().when(tableExtService).loadTable(result.get(0).getFirst(), result.get(0).getSecond(),
                "default");
        Mockito.doNothing().when(tableExtService).loadTable(result.get(1).getFirst(), result.get(1).getSecond(),
                "default");
        Mockito.doReturn(Lists.newArrayList(tableNames)).when(tableService).getSourceTableNames("default", "DEFAULT",
                "");
        Mockito.doReturn(Lists.newArrayList("DEFAULT")).when(tableService).getSourceDbNames("default");

        LoadTableResponse response = tableExtService.loadAWSTablesCompatibleCrossAccount(crossAccountTableReq,
                "default");
        Assert.assertEquals(2, response.getLoaded().size());

        KylinConfig.getInstanceFromEnv().setProperty("kylin.env.use-dynamic-S3-role-credential-in-table", "true");
        LoadTableResponse response2 = tableExtService.loadAWSTablesCompatibleCrossAccount(crossAccountTableReq,
                "default");
        Assert.assertEquals(2, response2.getLoaded().size());
    }

    @Test
    public void testUpdateAWSLoadedTableExtProp() {
        UpdateAWSTableExtDescRequest request = new UpdateAWSTableExtDescRequest();
        List<S3TableExtInfo> tableExtInfoList = new ArrayList<>();
        S3TableExtInfo s3TableExtInfo1 = new S3TableExtInfo();
        s3TableExtInfo1.setName("DEFAULT.TABLE0");
        s3TableExtInfo1.setLocation("s3://bucket1/test1/");
        S3TableExtInfo s3TableExtInfo2 = new S3TableExtInfo();
        s3TableExtInfo2.setName("DEFAULT.TABLE1");
        s3TableExtInfo2.setLocation("s3://bucket2/test2/");
        s3TableExtInfo2.setEndpoint("us-west-2.amazonaws.com");
        s3TableExtInfo2.setRoleArn("test:role");
        tableExtInfoList.add(s3TableExtInfo1);
        tableExtInfoList.add(s3TableExtInfo2);
        request.setProject("default");
        request.setTables(tableExtInfoList);

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setUuid(RandomUtil.randomUUIDStr());
        tableExtDesc.setIdentity("DEFAULT.TABLE1");
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName("TABLE1");
        tableDesc.setDatabase("DEFAULT");
        tableDesc.setUuid(RandomUtil.randomUUIDStr());
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        tableMetadataManager.saveTableExt(tableExtDesc);
        tableMetadataManager.saveSourceTable(tableDesc);

        UpdateAWSTableExtDescResponse response = tableExtService.updateAWSLoadedTableExtProp(request);
        Assert.assertEquals(1, response.getSucceed().size());

        KylinConfig.getInstanceFromEnv().setProperty("kylin.env.use-dynamic-S3-role-credential-in-table", "true");
        UpdateAWSTableExtDescResponse response2 = tableExtService.updateAWSLoadedTableExtProp(request);
        Assert.assertEquals(1, response2.getSucceed().size());
    }

    @Test
    public void testLoadTablesByDatabase() throws Exception {
        String[] tableIdentities = { "EDW.TEST_CAL_DT", "EDW.TEST_SELLER_TYPE_DIM", "EDW.TEST_SITES" };
        String[] tableNames = { "TEST_CAL_DT", "TEST_SELLER_TYPE_DIM", "TEST_SITES" };
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        List<Pair<TableDesc, TableExtDesc>> result = mockTablePair(3, "EDW");
        Mockito.doNothing().when(tableExtService).loadTable(result.get(1).getFirst(), result.get(1).getSecond(),
                "default");
        Mockito.doReturn(result).when(tableService).extractTableMeta(Mockito.any(), Mockito.any());
        loadTableResponse.setLoaded(Sets.newHashSet(tableIdentities));

        Mockito.doReturn(Lists.newArrayList(tableNames)).when(tableService).getSourceTableNames(Mockito.any(),
                Mockito.any(), Mockito.any());
        Mockito.doReturn(Lists.newArrayList("EDW")).when(tableService).getSourceDbNames("default");

        Mockito.doReturn(loadTableResponse).when(tableExtService).loadDbTables(tableIdentities, "default", false);
        LoadTableResponse response = tableExtService.loadDbTables(new String[] { "EDW" }, "default", true);

        Assert.assertEquals(1, response.getLoaded().size());
    }

    @Test
    public void testLoadTablesByDatabaseNotInCache() throws Exception {
        String[] tableIdentities = { "EDW.TEST_CAL_DT" };
        String[] tableNames = { "TEST_CAL_DT" };
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(Sets.newHashSet(tableIdentities));
        List<Pair<TableDesc, TableExtDesc>> result = mockTablePair(1, "EDW");

        Mockito.doReturn(Lists.newArrayList(tableNames)).when(tableService).getSourceTableNames("default", "EDW", "");
        Mockito.doReturn(loadTableResponse).when(tableExtService).loadDbTables(tableIdentities, "default", false);

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        tableManager.removeSourceTable("EDW.TEST_CAL_DT");
        Mockito.doReturn(Lists.newArrayList("EDW")).when(tableService).getSourceDbNames("default");
        Mockito.doReturn(result).when(tableService).extractTableMeta(Mockito.any(), Mockito.any());
        LoadTableResponse response = tableExtService.loadDbTables(new String[] { "EDW" }, "default", true);
        Assert.assertEquals(0, response.getLoaded().size());
    }

    @Test
    public void testRemoveJobIdFromTableExt() {
        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setUuid(RandomUtil.randomUUIDStr());
        tableExtDesc.setIdentity("DEFAULT.TEST_REMOVE");
        tableExtDesc.setJodID("test");
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName("TEST_REMOVE");
        tableDesc.setDatabase("DEFAULT");
        tableDesc.setUuid(RandomUtil.randomUUIDStr());
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        tableMetadataManager.saveTableExt(tableExtDesc);
        tableMetadataManager.saveSourceTable(tableDesc);
        tableExtService.removeJobIdFromTableExt("test", "default");
        TableExtDesc tableExtDesc1 = tableMetadataManager.getOrCreateTableExt("DEFAULT.TEST_REMOVE");
        Assert.assertNull(tableExtDesc1.getJodID());
    }

    private List<Pair<TableDesc, TableExtDesc>> mockTablePair(int size, String tableName) {
        List<Pair<TableDesc, TableExtDesc>> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            TableDesc table1 = new TableDesc();
            table1.setName(tableName + i);
            TableExtDesc tableExt1 = new TableExtDesc();
            result.add(Pair.newPair(table1, tableExt1));
        }
        return result;
    }

    private List<Pair<TableDesc, TableExtDesc>> mockTablePair(int size, String dbName, String tableName) {
        List<Pair<TableDesc, TableExtDesc>> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            TableDesc tableDesc = new TableDesc();
            tableDesc.setDatabase(dbName);
            tableDesc.setName(tableName + i);
            TableExtDesc tableExt1 = new TableExtDesc();
            result.add(Pair.newPair(tableDesc, tableExt1));
        }
        return result;
    }
}
