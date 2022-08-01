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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_FAIL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.constant.SnapshotStatus;
import org.apache.kylin.rest.request.SnapshotConfigRequest;
import org.apache.kylin.rest.request.SnapshotRequest;
import org.apache.kylin.rest.response.NInitTablesResponse;
import org.apache.kylin.rest.response.SnapshotCheckResponse;
import org.apache.kylin.rest.response.SnapshotColResponse;
import org.apache.kylin.rest.response.SnapshotInfoResponse;
import org.apache.kylin.rest.response.SnapshotPartitionsResponse;
import org.apache.kylin.rest.response.TableNameResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;

public class SnapshotServiceTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private SnapshotService snapshotService = Mockito.spy(new SnapshotService());

    @InjectMocks
    private ProjectService projectService = Mockito.spy(new ProjectService());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(IUserGroupService.class);

    @Mock
    protected TableService tableService = Mockito.spy(TableService.class);

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        createTestMetadata();
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "1");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(snapshotService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(snapshotService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(snapshotService, "tableService", tableService);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
    }

    @Test
    public void testBuildSnapshotWithoutSnapshotManualEnable() throws Exception {
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        final String table2 = "DEFAULT.TEST_ACCOUNT";
        Set<String> tables = Sets.newHashSet(table1, table2);
        thrown.expect(KylinException.class);
        thrown.expectMessage("Snapshot management is not enable");
        snapshotService.buildSnapshots(PROJECT, tables, Maps.newHashMap(), false, 3, null, null);
    }

    @Test
    public void testBuildSnapshotOfNoPermissionTables() throws Exception {
        enableSnapshotManualManagement();
        Set<String> tables = Sets.newHashSet("non-exist");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find table \"non-exist\". Please check and try again.");
        snapshotService.buildSnapshots(PROJECT, tables, Maps.newHashMap(), false, 3, null, null);
    }

    @Test
    public void testRefreshSnapshotFailWithNoSnapshot() throws Exception {
        enableSnapshotManualManagement();
        Set<String> tables = Sets.newHashSet("DEFAULT.TEST_KYLIN_FACT");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can't find the snapshot \"DEFAULT.TEST_KYLIN_FACT\". Please check and try again.");
        snapshotService.buildSnapshots(PROJECT, tables, Maps.newHashMap(), true, 3, null, null);
    }

    @Test
    public void testBuildSnapshot() throws Exception {
        enableSnapshotManualManagement();
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        final String table2 = "DEFAULT.TEST_ACCOUNT";
        Set<String> tables = Sets.newHashSet(table1, table2);
        Set<String> databases = Sets.newHashSet();
        snapshotService.buildSnapshots(PROJECT, databases, tables, Maps.newHashMap(), false, 0, null, null);

        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);

        final List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(2, allExecutables.size());

        final AbstractExecutable job1 = allExecutables.get(0);
        Assert.assertEquals(0, job1.getPriority());
        Assert.assertTrue(job1 instanceof NSparkSnapshotJob);
        NSparkSnapshotJob snapshotJob1 = (NSparkSnapshotJob) job1;
        Assert.assertEquals("SNAPSHOT_BUILD", snapshotJob1.getName());
        Assert.assertEquals(PROJECT, snapshotJob1.getProject());
        final String tableNameOfSamplingJob1 = snapshotJob1.getParam(NBatchConstants.P_TABLE_NAME);
        Assert.assertTrue(tables.contains(tableNameOfSamplingJob1));
        Assert.assertEquals(PROJECT, snapshotJob1.getParam(NBatchConstants.P_PROJECT_NAME));
        Assert.assertEquals("ADMIN", snapshotJob1.getSubmitter());

        final AbstractExecutable job2 = allExecutables.get(1);
        Assert.assertEquals(0, job2.getPriority());
        Assert.assertTrue(job2 instanceof NSparkSnapshotJob);
        NSparkSnapshotJob snapshotJob2 = (NSparkSnapshotJob) job2;
        Assert.assertEquals("SNAPSHOT_BUILD", snapshotJob2.getName());
        final String tableNameOfSamplingJob2 = snapshotJob2.getParam(NBatchConstants.P_TABLE_NAME);
        Assert.assertEquals(PROJECT, snapshotJob2.getProject());
        Assert.assertTrue(tables.contains(tableNameOfSamplingJob2));
        Assert.assertEquals(PROJECT, snapshotJob2.getParam(NBatchConstants.P_PROJECT_NAME));
        Assert.assertEquals("ADMIN", snapshotJob2.getSubmitter());
        Assert.assertEquals(tables, Sets.newHashSet(tableNameOfSamplingJob1, tableNameOfSamplingJob2));

        // refresh failed
        String expected = JOB_CREATE_CHECK_FAIL.getMsg();
        String actual = "";
        try {
            snapshotService.buildSnapshots(PROJECT, databases, tables, Maps.newHashMap(), true, 3, null, null);
        } catch (TransactionException e) {
            actual = e.getCause().getMessage();
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testFixBrokenSnapshot() {
        enableSnapshotManualManagement();
        String partColName = "CAL_DT";
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
            TableDesc copy = tbgr.copyForWrite(tbgr.getTableDesc(tableName));
            copy.setSnapshotHasBroken(true);
            tbgr.updateTableDesc(copy);
            return null;
        }, PROJECT);
        TableDesc table = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableName);
        Assert.assertTrue(table.isSnapshotHasBroken());
        snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(), Sets.newHashSet(tableName), Maps.newHashMap(), false,
                1, null, null);
        table = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableName);
        Assert.assertTrue(!table.isSnapshotHasBroken());
    }

    @Test
    public void testBuildSnapshotOfDatabase() throws Exception {
        // build snapshots of database "DEFAULT"
        enableSnapshotManualManagement();
        final String database = "DEFAULT";
        Set<String> databases = Sets.newHashSet(database);
        Set<String> tables = Sets.newHashSet();
        snapshotService.buildSnapshots(PROJECT, databases, tables, Maps.newHashMap(), false, 3, null, null);
        val executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        final List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        long expectedTableSize = tableManager.listAllTables().stream()
                .filter(tableDesc -> tableDesc.getDatabase().equals(database)).count();
        Assert.assertEquals(expectedTableSize, allExecutables.size());

        // build snapshots of non-exist database
        databases = Sets.newHashSet("non-exist");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find database \"NON-EXIST\". Please check and try again.");
        snapshotService.buildSnapshots(PROJECT, databases, tables, Maps.newHashMap(), false, 3, null, null);
    }

    @Test
    public void testBuildSnapshotByPartition() throws Exception {
        enableSnapshotManualManagement();
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        final String table2 = "DEFAULT.TEST_ACCOUNT";
        String partitionCol = "CAL_DT";

        tableService = Mockito.mock(TableService.class);
        ReflectionTestUtils.setField(snapshotService, "tableService", tableService);
        NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        TableDesc tableDesc = tbgr.copyForWrite(tbgr.getTableDesc(table1));
        tableDesc.setSourceType(1);
        tableDesc.setPartitionColumn(partitionCol);

        Mockito.when(tableService.extractTableMeta(Arrays.asList(table1).toArray(new String[0]), PROJECT))
                .thenReturn(Arrays.asList(Pair.newPair(tableDesc, null)));

        Set<String> tables = Sets.newHashSet(table1, table2);
        Set<String> databases = Sets.newHashSet();

        SnapshotRequest.TableOption option = new SnapshotRequest.TableOption();
        option.setPartitionCol(partitionCol);
        option.setIncrementalBuild(true);
        snapshotService.buildSnapshots(PROJECT, databases, tables, ImmutableMap.of(table1, option), false, 3, null,
                null);

        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);

        final List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(2, allExecutables.size());

        final AbstractExecutable job1 = allExecutables.get(0);
        Assert.assertTrue(job1 instanceof NSparkSnapshotJob);
        NSparkSnapshotJob samplingJob1 = (NSparkSnapshotJob) job1;
        Assert.assertEquals("SNAPSHOT_BUILD", samplingJob1.getName());
        Assert.assertEquals(PROJECT, samplingJob1.getProject());
        final String tableNameOfSamplingJob1 = samplingJob1.getParam(NBatchConstants.P_TABLE_NAME);
        Assert.assertTrue(tables.contains(tableNameOfSamplingJob1));
        Assert.assertEquals(PROJECT, samplingJob1.getParam(NBatchConstants.P_PROJECT_NAME));
        Assert.assertEquals("ADMIN", samplingJob1.getSubmitter());

    }

    @Test
    public void testBuildSnapshotByPartitionWithInvalidPartitionsToBuild() {
        enableSnapshotManualManagement();
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        final String table2 = "DEFAULT.TEST_ACCOUNT";
        String partitionCol = "CAL_DT";

        tableService = Mockito.mock(TableService.class);
        ReflectionTestUtils.setField(snapshotService, "tableService", tableService);
        NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        TableDesc tableDesc = tbgr.copyForWrite(tbgr.getTableDesc(table1));
        tableDesc.setSourceType(1);
        tableDesc.setPartitionColumn(partitionCol);

        Mockito.when(tableService.extractTableMeta(Arrays.asList(table1).toArray(new String[0]), PROJECT))
                .thenReturn(Arrays.asList(Pair.newPair(tableDesc, null)));

        Set<String> tables = Sets.newHashSet(table1, table2);
        Set<String> databases = Sets.newHashSet();

        SnapshotRequest.TableOption option1 = new SnapshotRequest.TableOption();
        option1.setPartitionCol(partitionCol);
        option1.setIncrementalBuild(true);
        option1.setPartitionsToBuild(Sets.newHashSet());

        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Please select at least one partition for the following snapshots when conducting custom partition value refresh: [DEFAULT.TEST_KYLIN_FACT]");
        snapshotService.buildSnapshots(PROJECT, databases, tables, ImmutableMap.of(table1, option1), false, 3, null,
                null);
    }

    @Test
    public void testSamplingKillAnExistingNonFinalJob() throws Exception {
        enableSnapshotManualManagement();
        // initialize a sampling job and assert the status of it
        String table = "DEFAULT.TEST_KYLIN_FACT";
        snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(table), Maps.newHashMap(), false, 3, null, null);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(1, allExecutables.size());

        val initialJob = allExecutables.get(0);
        Assert.assertEquals(ExecutableState.READY, initialJob.getStatus());
        try {
            snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(table), Maps.newHashMap(), false, 3, null, null);
        } catch (TransactionException e) {
            Assert.assertTrue(e.getCause() instanceof JobSubmissionException);
            Assert.assertEquals(JOB_CREATE_CHECK_FAIL.getMsg(), (e.getCause()).getMessage());
        }
    }

    @Test
    public void testDeleteSnapshot() {
        enableSnapshotManualManagement();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        tableManager.getTableDesc(tableName).setLastSnapshotPath("file://a/b");
        Assert.assertNotNull(getSnapshotPath(tableName));
        snapshotService.deleteSnapshots(PROJECT, Sets.newHashSet(tableName));
        Assert.assertNull(getSnapshotPath(tableName));
        Assert.assertEquals(-1, getOriginalSize(tableName));
    }

    @Test
    public void testDeleteSnapshotWithRunningSnapshot() throws Exception {
        enableSnapshotManualManagement();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        tableManager.getTableDesc(tableName).setLastSnapshotPath("file://a/b");
        Assert.assertNotNull(getSnapshotPath(tableName));
        snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(tableName), Maps.newHashMap(), false, 3, null, null);
        SnapshotCheckResponse response = snapshotService.deleteSnapshots(PROJECT, Sets.newHashSet(tableName));
        Assert.assertEquals(1, response.getAffectedJobs().size());
        Assert.assertNull(getSnapshotPath(tableName));
    }

    @Test
    public void testCheckBeforeDeleteSnapshotWithRunningSnapshot() throws Exception {
        enableSnapshotManualManagement();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        tableManager.getTableDesc(tableName).setLastSnapshotPath("file://a/b");
        Assert.assertNotNull(getSnapshotPath(tableName));
        snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(tableName), Maps.newHashMap(), false, 3, null, null);
        SnapshotCheckResponse response = snapshotService.checkBeforeDeleteSnapshots(PROJECT,
                Sets.newHashSet(tableName));
        Assert.assertEquals(1, response.getAffectedJobs().size());
        Assert.assertNotNull(getSnapshotPath(tableName));
    }

    @Test
    public void testGetProjectSnapshots() {
        enableSnapshotManualManagement();
        String tablePattern = "SSB";
        Set<SnapshotStatus> statusFilter = Sets.newHashSet(SnapshotStatus.ONLINE);
        String sortBy = "";
        setSnapshotPath("SSB.LINEORDER", "some_path");
        setSnapshotPath("SSB.P_LINEORDER", "some_path");
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("testuser", "testuser", Constant.ROLE_MODELER));
        List<SnapshotInfoResponse> responses = snapshotService.getProjectSnapshots(PROJECT, tablePattern, statusFilter,
                Sets.newHashSet(), sortBy, true);
        Assert.assertEquals(0, responses.size());
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        responses = snapshotService.getProjectSnapshots(PROJECT, tablePattern, statusFilter, Sets.newHashSet(), sortBy,
                true);
        SnapshotInfoResponse response = responses.get(0);
        Assert.assertEquals(2, responses.size());
        Assert.assertEquals("SSB", response.getDatabase());
        Assert.assertEquals(Sets.newHashSet("LINEORDER", "P_LINEORDER"),
                responses.stream().map(SnapshotInfoResponse::getTable).collect(Collectors.toSet()));
    }

    @Test
    public void testGetTables() {
        enableSnapshotManualManagement();
        String tablePattern = "SSB.CUSTOM";
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("testuser", "testuser", Constant.ROLE_MODELER));
        snapshotService.getTables(PROJECT, tablePattern, 0, 10);

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        NInitTablesResponse response = snapshotService.getTables(PROJECT, tablePattern, 0, 1);
        NInitTablesResponse.DatabaseTables database = response.getDatabases().get(0);
        Assert.assertEquals("SSB", database.getDbname());
        Assert.assertEquals("CUSTOMER", ((TableNameResponse) database.getTables().get(0)).getTableName());
        Assert.assertEquals(false, ((TableNameResponse) database.getTables().get(0)).isLoaded());
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        response = snapshotService.getTables("streaming_test", "", 0, Integer.MAX_VALUE);
        Assert.assertEquals(1, response.getDatabases().size());
        Assert.assertEquals(1, database.getTables().size());
    }

    @Test
    public void TestGetTableNameResponses() {
        enableSnapshotManualManagement();
        String database = "SSB";
        String tablePattern = "ABC.CUSTOM";
        int size = snapshotService.getTableNameResponses(PROJECT, database, tablePattern).size();
        Assert.assertEquals(0, size);

        tablePattern = "SSB.CUSTOM";
        TableNameResponse response = snapshotService.getTableNameResponses(PROJECT, database, tablePattern).get(0);
        Assert.assertEquals("CUSTOMER", response.getTableName());
        Assert.assertEquals(false, response.isLoaded());

        tablePattern = "";
        List<TableNameResponse> responses = snapshotService.getTableNameResponses(PROJECT, database, tablePattern);
        response = responses.get(0);
        Assert.assertEquals("CUSTOMER", response.getTableName());
        Assert.assertEquals(false, response.isLoaded());
        Assert.assertEquals(6, responses.size());

        tablePattern = null;
        responses = snapshotService.getTableNameResponses(PROJECT, database, tablePattern);
        response = responses.get(0);
        Assert.assertEquals("CUSTOMER", response.getTableName());
        Assert.assertEquals(false, response.isLoaded());
        Assert.assertEquals(6, responses.size());
    }

    @Test
    public void testConfigPartitionCol() {
        enableSnapshotManualManagement();
        String partColName = "CAL_DT";
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        snapshotService.configSnapshotPartitionCol(PROJECT,
                ImmutableMap.<String, String> builder().put(tableName, partColName).build());
        NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        TableDesc tableDesc = tbgr.getTableDesc(tableName);
        Assert.assertEquals(partColName, tableDesc.getSelectedSnapshotPartitionCol());
    }

    @Test
    public void testConfigPartitionColNotSupportSource() {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "8");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        enableSnapshotManualManagement();
        String partColName = "CAL_DT";
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        thrown.expect(KylinException.class);
        snapshotService.configSnapshotPartitionCol(PROJECT,
                ImmutableMap.<String, String> builder().put(tableName, partColName).build());
        thrown.expectMessage("not support");

    }

    @Test
    public void testConfigNonExistPartitionCol() {
        enableSnapshotManualManagement();
        thrown.expect(KylinException.class);
        String partColName = "ABC";
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        snapshotService.configSnapshotPartitionCol(PROJECT,
                ImmutableMap.<String, String> builder().put(tableName, partColName).build());
        thrown.expectMessage("not exist");
    }

    @Test
    public void testConfigNonPartitionCol() {
        enableSnapshotManualManagement();
        try {
            snapshotService.configSnapshotPartitionCol(PROJECT, ImmutableMap.<String, String> builder().build());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorCode().getCode(),
                    ((KylinException) e).getErrorCode().getCodeString());
            Assert.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("table_partition_col"), e.getMessage());
            Assert.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorSuggest().getString(),
                    ((KylinException) e).getSuggestionString());
        }
    }

    @Test
    public void testGetAllSnapshotCol() {
        enableSnapshotManualManagement();
        List<SnapshotColResponse> response = snapshotService.getSnapshotCol(PROJECT, null, null, null, false);
        Assert.assertEquals(true, response.size() > 0);
        response.forEach(table -> Assert.assertEquals(null, table.getPartitionCol()));
    }

    @Test
    public void testGetSnapshotCol() {
        enableSnapshotManualManagement();
        String partColName = "CAL_DT";
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
            TableDesc copy = tbgr.copyForWrite(tbgr.getTableDesc(tableName));
            copy.setPartitionColumn(partColName);
            tbgr.updateTableDesc(copy);
            return null;
        }, PROJECT);

        List<SnapshotColResponse> response = snapshotService.getSnapshotCol(PROJECT, ImmutableSet.of(tableName), null,
                null, false);
        Assert.assertEquals(1, response.size());
        Assert.assertEquals(partColName, response.get(0).getPartitionCol());
    }

    @Test
    public void testGetBrokenSnapshotCol() {
        enableSnapshotManualManagement();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
            TableDesc copy = tbgr.copyForWrite(tbgr.getTableDesc(tableName));
            copy.setSnapshotHasBroken(true);
            tbgr.updateTableDesc(copy);
            return null;
        }, PROJECT);
        List<SnapshotColResponse> responses = snapshotService.getSnapshotCol(PROJECT, null, Sets.newHashSet("DEFAULT"),
                null, false, true);
        Assert.assertEquals(10, responses.size());
        responses = snapshotService.getSnapshotCol(PROJECT, null, Sets.newHashSet("DEFAULT"), null, false, false);
        Assert.assertEquals(11, responses.size());
        responses = snapshotService.getSnapshotCol(PROJECT, Sets.newHashSet("DEFAULT.TEST_KYLIN_FACT"), null, null,
                false, false);
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals("TEST_KYLIN_FACT", responses.get(0).getTable());
    }

    @Test
    public void testGetBrokenSnapshot() {
        enableSnapshotManualManagement();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
            TableDesc copy = tbgr.copyForWrite(tbgr.getTableDesc(tableName));
            copy.setSnapshotHasBroken(true);
            tbgr.updateTableDesc(copy);
            return null;
        }, PROJECT);
        List<SnapshotInfoResponse> responses = snapshotService.getProjectSnapshots(PROJECT, null,
                Sets.newHashSet(SnapshotStatus.BROKEN), Sets.newHashSet(), null, true);
        Assert.assertEquals(1, responses.size());
    }

    @Test
    public void testReloadPartitionCol() throws Exception {
        enableSnapshotManualManagement();
        tableService = Mockito.mock(TableService.class);
        ReflectionTestUtils.setField(snapshotService, "tableService", tableService);
        String partColName = "CAL_DT";
        String[] tableName = { "DEFAULT.TEST_KYLIN_FACT" };
        NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        TableDesc tableDesc = tbgr.getTableDesc(tableName[0]);
        tableDesc.setPartitionColumn(partColName);
        Mockito.when(tableService.extractTableMeta(tableName, PROJECT))
                .thenReturn(Arrays.asList(Pair.newPair(tableDesc, null)));
        SnapshotColResponse response = snapshotService.reloadPartitionCol(PROJECT, tableName[0]);
        Assert.assertEquals(partColName, response.getPartitionCol());
    }

    @Test
    public void testGetPartitions() {
        String project = "default";
        Map<String, String> map = Maps.newHashMap();
        map.put("SSB.SUPPLIER", "S_NATION");
        map.put("SSB.DATES", "S_NATION");
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tbgr = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
            TableDesc tableDesc = tbgr.getTableDesc("SSB.SUPPLIER");
            TableDesc tableDescCopy = tbgr.copyForWrite(tableDesc);
            tableDescCopy.setPartitionColumn("S_NATION");
            tableDesc.setSourceType(9);
            tbgr.saveSourceTable(tableDescCopy);
            return null;
        }, project);

        SnapshotConfigRequest request = new SnapshotConfigRequest();
        request.setSnapshotManualManagementEnabled(true);
        projectService.updateSnapshotConfig(project, request);
        Map<String, SnapshotPartitionsResponse> responses = snapshotService.getPartitions(project, map);
        assert responses.get("SSB.SUPPLIER").getNotReadyPartitions().size() == 2;
        assert responses.get("SSB.SUPPLIER").getReadyPartitions().size() == 0;
        assert responses.get("SSB.DATES") == null;
    }

    private String getSnapshotPath(String tableName) {
        return NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableName)
                .getLastSnapshotPath();
    }

    private void setSnapshotPath(String tableName, String snapshotPath) {
        NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableName)
                .setLastSnapshotPath(snapshotPath);
    }

    private long getOriginalSize(String tableName) {
        return NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getOrCreateTableExt(tableName)
                .getOriginalSize();
    }

    private void enableSnapshotManualManagement() {
        SnapshotConfigRequest request = new SnapshotConfigRequest();
        request.setSnapshotManualManagementEnabled(true);
        projectService.updateSnapshotConfig(PROJECT, request);
        projectService.updateSnapshotConfig("streaming_test", request);
    }
}
