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

import static org.apache.kylin.common.exception.QueryErrorCode.EMPTY_TABLE;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.builder.InternalTableLoader;
import org.apache.kylin.engine.spark.job.InternalTableLoadJob;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.metadata.table.InternalTablePartitionDetail;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.InternalTableDescResponse;
import org.apache.kylin.rest.response.InternalTableLoadingJobResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import io.delta.tables.ClickhouseTable;
import lombok.val;

@MetadataInfo
public class InternalTableServiceTest extends AbstractTestCase {

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Spy
    private InternalTableLoadingService internalTableLoadingService = Mockito.spy(new InternalTableLoadingService());
    @InjectMocks
    private InternalTableService internalTableService = Mockito.spy(new InternalTableService());

    @InjectMocks
    private TableService tableService = mock(TableService.class);

    static final String PROJECT = "default";
    static final String TABLE_INDENTITY = "DEFAULT.TEST_KYLIN_FACT";
    static final String DATE_COL = "CAL_DT";
    static final String INTERNAL_DIR = PROJECT + "/Internal/" + TABLE_INDENTITY.replace(".", "/");
    static final String BASE_SQL = "select * from INTERNAL_CATALOG." + PROJECT + "." + TABLE_INDENTITY;

    @BeforeAll
    public static void beforeClass() {
        NLocalWithSparkSessionTestBase.beforeClass();
    }

    @AfterAll
    public static void afterClass() {
        NLocalWithSparkSessionTestBase.afterClass();
    }

    @BeforeEach
    void setUp() throws Exception {
        JobContextUtil.cleanUp();
        MockitoAnnotations.openMocks(this);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        SparkJobFactoryUtils.initJobFactory();
        overwriteSystemProp("kylin.source.provider.9", "org.apache.kylin.engine.spark.mockup.CsvSource");
    }

    @Test
    void testCheckParams() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        // null value is valid
        internalTableService.checkParameters(null, table, null);

        // empty array & blank string are valid too
        String[] partitionCols = new String[] {};
        String datePartitionFormat = "";
        internalTableService.checkParameters(partitionCols, table, datePartitionFormat);

        // partitionCols are case insensitive
        partitionCols = new String[] { "trans_id", "order_id" };
        internalTableService.checkParameters(partitionCols, table, null);

        // when datePartitionFormat is null, non-date cols can be used as partitionCol
        internalTableService.checkParameters(partitionCols, table, "");

        // test partitionCols include date, but datePartitionFormat is null
        Assertions.assertThrows(KylinException.class,
                () -> internalTableService.checkParameters(new String[] { "CAL_DT" }, table, ""));

        // test datePartitionFormat is not null, but partitionCols is null
        Assertions.assertThrows(KylinException.class,
                () -> internalTableService.checkParameters(null, table, "yyyy-MM-dd"));

        // test datePartitionFormat is not null, but partitionCols don't contains data type col
        Assertions.assertThrows(KylinException.class, () -> internalTableService
                .checkParameters(new String[] { "TRANS_ID", "order_id" }, table, "yyyy-MM-dd"));

        // test invalid partitionCols
        Assertions.assertThrows(KylinException.class, () -> internalTableService
                .checkParameters(new String[] { "TRANS_ID", "order_id_2" }, table, "yyyy-MM-dd"));

        // test invalid partitionCols
        Assertions.assertThrows(KylinException.class,
                () -> internalTableService.checkParameters(new String[] { "TRANS_ID", "CAL_DT" }, table, "yyyy-mm"));

    }

    @Test
    void testCheckParamsWithEmptySourceTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenThrow(new KylinException(
                EMPTY_TABLE, String.format(Locale.ROOT, MsgPicker.getMsg().getNoDataInTable(), table)));
        // test right date format with source table data is empty
        internalTableService.checkParameters(new String[] { "TRANS_ID", "CAL_DT" }, table, "yyyy-MM-dd");
    }

    @Test
    void testCheckParamsWithFatalError() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any()))
                .thenThrow(new IllegalStateException("fatal error"));
        Assertions.assertThrows(IllegalStateException.class,
                () -> internalTableService.checkParameters(new String[] { "TRANS_ID", "CAL_DT" }, table, "yyyy-MM-dd"));
    }

    @Test
    void testCreateInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);

        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertTrue(internalTableFolder.exists() && internalTableFolder.isDirectory());

        // test create duplicated internal table
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(),
                        partitionCols, "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

        // test create internal table without tableDesc
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.createInternalTable(PROJECT, table.getName() + "_xxx", table.getDatabase(),
                        partitionCols, "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

        // test drop internal table without data dir
        if (!internalTableFolder.delete()) {
            Assertions.fail();
        }
        internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY);
    }

    @Test
    void testCreateDeltaInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.DELTALAKE.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertTrue(internalTableFolder.exists() && internalTableFolder.isDirectory());
        // test create duplicated internal table
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(),
                        partitionCols, "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.DELTALAKE.name()));
        // test create internal table without tableDesc
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.createInternalTable(PROJECT, table.getName() + "_xxx", table.getDatabase(),
                        partitionCols, "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.DELTALAKE.name()));
        // test create delta table with errors
        Assertions.assertThrows(Exception.class, () -> {
            try (MockedConstruction<InternalTableLoader> mocked = Mockito.mockConstruction(InternalTableLoader.class,
                    (mock, context) -> {
                        doThrow(new Exception()).when(mock).loadInternalTable(any(), any(), anyString(), anyString(),
                                anyString(), anyString(), anyBoolean());
                    })) {
                InternalTableDesc tmpInternal = new InternalTableDesc();
                tmpInternal.setStorageType(InternalTableDesc.StorageType.DELTALAKE.name());
                internalTableService.createDeltaSchema(tmpInternal);
            }
        });
        internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY);
    }

    @Test
    void testUpdateInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), new String[] {}, null,
                new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNull(internalTable.getTablePartition());
        Assertions.assertTrue(internalTable.getTblProperties().isEmpty());

        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        tblProperties.put("orderByKeys", "LO_ORDERKEY");
        tblProperties.put("primaryKey", "LO_ORDERKEY2");
        String dateFormat = "yyyy-MM-dd";
        internalTableService.updateInternalTable(PROJECT, internalTable.getName(), internalTable.getDatabase(),
                partitionCols, dateFormat, tblProperties, InternalTableDesc.StorageType.PARQUET.name());

        // check internal table metadata
        List<InternalTableDescResponse> internalTables = internalTableService.getTableList(PROJECT, false, false, "",
                "");
        Assertions.assertEquals(1, internalTables.size());
        InternalTableDescResponse response = internalTables.get(0);
        Assertions.assertEquals(DATE_COL, response.getTimePartitionCol());

        // check internal table details
        List<InternalTablePartitionDetail> details = internalTableService.getTableDetail(PROJECT,
                internalTable.getDatabase(), internalTable.getName());
        Assertions.assertNull(details);

        // test set partitionCols to null
        internalTableService.updateInternalTable(PROJECT, internalTable.getName(), internalTable.getDatabase(), null,
                "", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNull(internalTable.getPartitionColumns());

        // test set partitionCols to empty
        internalTableService.updateInternalTable(PROJECT, internalTable.getName(), internalTable.getDatabase(),
                new String[] {}, "", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNull(internalTable.getPartitionColumns());

        // test update an internal table without create
        String db = internalTable.getDatabase();
        String tableName = internalTable.getName();
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.updateInternalTable(PROJECT, "TEST_ACCOUNT", db, partitionCols, dateFormat,
                        tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

        // test update an internal table which has loaded data.
        UnitOfWork.doInTransactionWithRetry(() -> {
            InternalTableManager manager = InternalTableManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
            manager.updateInternalTable(TABLE_INDENTITY, copyForWrite -> copyForWrite.setRowCount(1L));
            return null;
        }, PROJECT);

        Assertions.assertThrows(TransactionException.class, () -> internalTableService.updateInternalTable(PROJECT,
                tableName, db, partitionCols, dateFormat, tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

    }

    @Test
    void testReloadInternalTableSchema() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);

        try {
            internalTableService.reloadInternalTableSchema(PROJECT, TABLE_INDENTITY);
        } catch (Exception e) {
            Assertions.fail("Expect no exception.", e);
        }

        internalTableService.createInternalTable(PROJECT, table, InternalTableDesc.StorageType.PARQUET.name());
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        // mock file under internal table folder
        File tmpFile = new File(internalTableFolder, "mocked_file");
        boolean created = tmpFile.createNewFile();
        Assertions.assertTrue(created);
        Assertions.assertEquals(1, internalTableFolder.list().length);

        internalTableService.reloadInternalTableSchema(PROJECT, TABLE_INDENTITY);
        Assertions.assertEquals(0, internalTableFolder.list().length);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        InternalTableDesc internalTableDesc = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        internalTableDesc.setRowCount(1);
        internalTableManager.saveOrUpdateInternalTable(internalTableDesc);
        Assert.assertThrows(KylinException.class,
                () -> internalTableService.reloadInternalTableSchema(PROJECT, TABLE_INDENTITY));

    }

    @Test
    void testLoadAndDeleteInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);

        internalTableService.createInternalTable(PROJECT, table, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableLoadingJobResponse response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(),
                table.getDatabase(), false, false, "", "", null);
        String jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);

        // check internal table data exist
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertEquals(1, Objects.requireNonNull(internalTableFolder.list()).length);

        // check query
        SparkSession ss = SparderEnv.getSparkSession();
        Assertions.assertFalse(ss.sql(BASE_SQL).isEmpty());

        // check truncate，will delete old path and not create empty schema for parquet format
        internalTableService.truncateInternalTable(PROJECT, TABLE_INDENTITY);
        Assertions.assertFalse(internalTableFolder.exists());

        // test truncate nonexistent internal tables
        Assertions.assertThrows(KylinException.class,
                () -> internalTableService.truncateInternalTable(PROJECT, TABLE_INDENTITY + "2"));

        // check delete
        internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY);
        Assertions.assertFalse(internalTableFolder.exists());

        // test drop an internal table twice
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY));
    }

    @Test
    void testLoadAndDeleteInternalTableWithIncremental() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, TABLE_INDENTITY, new String[] { DATE_COL }, "yyyy-MM-dd",
                new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());
        String startDate = "1325347200000"; // 2012-01-01
        String endDate = "1325865600000"; // 2012-01-07
        InternalTableLoadingJobResponse response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(),
                table.getDatabase(), true, false, startDate, endDate, null);
        String jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);

        // check internal table data exist
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertEquals(6, internalTableFolder.list().length);

        // check query
        SparkSession ss = SparderEnv.getSparkSession();
        long count = ss.sql(BASE_SQL).count();
        Assertions.assertTrue(count > 0);

        // refresh all loaded table
        endDate = "1325779200000"; // 2012-01-06
        response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(), table.getDatabase(), true, true,
                startDate, endDate, null);
        Assert.assertFalse(response.getJobs().isEmpty());
        jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);
        // check refresh time out of loaded range
        Assertions.assertThrows(Exception.class, () -> internalTableService.loadIntoInternalTable(PROJECT,
                table.getName(), table.getDatabase(), false, true, "1316556800000", "", null));// 2011-09-21 ~ ~
        Assertions.assertThrows(Exception.class, () -> internalTableService.loadIntoInternalTable(PROJECT,
                table.getName(), table.getDatabase(), false, true, "1326556800000", "", null));// 2012-01-15 ~ ~
        Assertions.assertThrows(Exception.class, () -> internalTableService.loadIntoInternalTable(PROJECT,
                table.getName(), table.getDatabase(), false, true, "", "1325865600000", null));// ~ ~ 2012-01-07
        Assertions.assertThrows(Exception.class, () -> internalTableService.loadIntoInternalTable(PROJECT,
                table.getName(), table.getDatabase(), false, true, startDate, "1326556800000", null));// 2012-01-01 ~ 2012-01-15
        Assertions.assertThrows(Exception.class, () -> internalTableService.loadIntoInternalTable(PROJECT,
                table.getName(), table.getDatabase(), false, true, startDate, "1293811200000", null));// 2012-01-01 ~ 2011-01-01

        // refresh some partitions and check agine
        String middleDate = "1325520000000";
        response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(), table.getDatabase(), true, true,
                startDate, middleDate, null);
        jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);
        Assertions.assertEquals(count, ss.sql(BASE_SQL).count());

        // remove some partitions and check
        String[] toDeletePartitions = new String[] { "2012-01-03", "2012-01-04" };
        internalTableService.dropPartitionsOnDeltaTable(PROJECT, TABLE_INDENTITY, toDeletePartitions, null);
        Assertions.assertEquals(6 - toDeletePartitions.length, internalTableFolder.list().length);
        long newCount = ss.sql(BASE_SQL).count();
        Assertions.assertTrue(newCount > 0 && newCount < count);

        // remove partitions not exist in table
        String[] toDeletePartitionsNotExist = new String[] { "2013-01-03", "2013-01-04" };
        Assert.assertThrows(KylinException.class, () -> {
            internalTableService.dropPartitionsOnDeltaTable(PROJECT, TABLE_INDENTITY, toDeletePartitionsNotExist, null);
        });

        // check delete table
        internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY);
        Assertions.assertFalse(internalTableFolder.exists());
    }

    @Test
    void testDropNotExistsTablePartition() {
        // remove some partitions and check
        String[] toDeletePartitions = new String[] { "2012-01-03", "2012-01-04" };
        Assert.assertThrows(KylinException.class, () -> {
            internalTableService.dropPartitionsOnDeltaTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT_NOT", toDeletePartitions,
                    null);
        });
    }

    @Test
    void testDropNonPartitionTablePartition() throws Exception {
        internalTableService.createInternalTable(PROJECT, TABLE_INDENTITY, null, null, new HashMap<>(),
                InternalTableDesc.StorageType.PARQUET.name());
        String[] toDeletePartitions = new String[] { "2012-01-03", "2012-01-04" };
        Assert.assertThrows(KylinException.class, () -> {
            internalTableService.dropPartitionsOnDeltaTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", toDeletePartitions,
                    null);
        });
    }

    @Test
    void testTruncatePartitionInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableLoadingJobResponse response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(),
                table.getDatabase(), false, false, "", "", null);
        String jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);

        InternalTableDesc internalTableDesc = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        // check internal table data exist
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        // parquet format not have meta dir, so the partition size should equal file size
        Assertions.assertEquals(internalTableDesc.getTablePartition().getPartitionDetails().size(),
                Objects.requireNonNull(internalTableFolder.list()).length);

        // check truncate，will delete old path and not create empty schema for parquet format
        internalTableService.truncateInternalTable(PROJECT, TABLE_INDENTITY);
        Assertions.assertFalse(internalTableFolder.exists());
        InternalTableDesc afterTruncateTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertEquals(-1, afterTruncateTable.getStorageSize());
    }

    @Disabled("gluten not support deltaLake yet.")
    @Test
    void testTruncatePartitionDeltaInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.DELTALAKE.name());
        InternalTableLoadingJobResponse response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(),
                table.getDatabase(), false, false, "", "", null);
        String jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);

        InternalTableDesc internalTableDesc = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        // check internal table data exist
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        // delta format have meta dir, so the partition size + 1 should equal file size
        Assertions.assertEquals(internalTableDesc.getTablePartition().getPartitionDetails().size() + 1,
                Objects.requireNonNull(internalTableFolder.list()).length);

        // check truncate，will create a new empty schema dir for delta format
        internalTableService.truncateInternalTable(PROJECT, TABLE_INDENTITY);
        Assertions.assertTrue(internalTableFolder.exists());
        InternalTableDesc afterTruncateTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertTrue(afterTruncateTable.getStorageSize() > 0);
    }

    private void waitJobToFinished(KylinConfig config, String jobId) {
        ExecutableManager executableManager = ExecutableManager.getInstance(config, PROJECT);
        await().atMost(10, TimeUnit.MINUTES).until(() -> {
            ExecutableState state = executableManager.getJob(jobId).getStatus();
            return state.isFinalState() || state == ExecutableState.ERROR;
        });
        // check job
        Assertions.assertEquals(ExecutableState.SUCCEED, executableManager.getJob(jobId).getStatus());
    }

    @Test
    void testGetTableListAndDetails() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");

        List<InternalTablePartitionDetail> details = null;
        KylinException notExistException = null;
        try {
            details = internalTableService.getTableDetail(PROJECT, table.getDatabase(), table.getName());
        } catch (KylinException e) {
            notExistException = e;
        }
        Assertions.assertNull(details);
        Assertions.assertTrue(
                null != notExistException && notExistException.getErrorCode().getCodeString().equals("KE-010007014"));

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), null, null,
                new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());

        List<InternalTableDescResponse> tables = internalTableService.getTableList(PROJECT, false, false, "", "");
        Assertions.assertEquals(1, tables.size());
        Assertions.assertNull(tables.get(0).getTimePartitionCol());

        details = internalTableService.getTableDetail(PROJECT, table.getDatabase(), table.getName());
        Assertions.assertNull(details);

        internalTableService.updateInternalTable(PROJECT, tables.get(0).getTableName(), tables.get(0).getDatabaseName(),
                new String[] { DATE_COL }, "yyyy-MM-dd", new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());

        tables = internalTableService.getTableList(PROJECT, false, false, "", "");
        Assertions.assertEquals(1, tables.size());
        Assertions.assertEquals(DATE_COL, tables.get(0).getTimePartitionCol());

        details = internalTableService.getTableDetail(PROJECT, table.getDatabase(), table.getName());
        Assertions.assertNull(details);
    }

    @Test
    void testLoadWithTblProperties() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        InternalTableLoader loader = new InternalTableLoader();
        SparkSession ss = SparderEnv.getSparkSession();

        HashMap<String, String> tblProperties = new HashMap<>();
        tblProperties.put("primaryKey", null);
        tblProperties.put("orderByKey", null);
        tblProperties.put("bucketCol", null);
        tblProperties.put("bucketNum", null);

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), null, null,
                tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);

        // load without additional parameters
        loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false);

        // load with bucketCol but without bucketNum
        tblProperties.put("bucketCol", DATE_COL);
        internalTable.setTblProperties(tblProperties);
        Assertions.assertThrows(KylinException.class,
                () -> loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false));

        // load with bucketCol && bucketNum
        tblProperties.put("bucketNum", "1");
        try {
            loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false);
        } catch (Exception e) {
            Assertions.fail();
        }

        // load with orderByKey
        tblProperties.put("orderByKey", "TRAND_ID");
        try {
            loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false);
        } catch (Exception e) {
            Assertions.fail();
        }
        // load with orderByKey && primaryKey
        tblProperties.put("primaryKey", "TRAND_ID");
        try {
            loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    void testCreateExistInternalTableErrorCode() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);

        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertTrue(internalTableFolder.exists() && internalTableFolder.isDirectory());

        // test create duplicated internal table to trigger error
        TransactionException exception = Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(),
                        partitionCols, "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name()));
        Assertions.assertEquals(
                String.format(Locale.ROOT, MsgPicker.getMsg().getSameInternalTableNameExist(), table.getName()),
                exception.getCause().getMessage());
        if (!internalTableFolder.delete()) {
            Assertions.fail();
        }

        internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY);
    }

    @Test
    void testUpdateNonEmptyInternalTableErrorCode() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), new String[] {}, null,
                new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNull(internalTable.getTablePartition());
        Assertions.assertTrue(internalTable.getTblProperties().isEmpty());

        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        tblProperties.put("orderByKeys", "LO_ORDERKEY");
        tblProperties.put("primaryKey", "LO_ORDERKEY2");
        String dateFormat = "yyyy-MM-dd";

        String db = internalTable.getDatabase();
        String tableName = internalTable.getName();

        UnitOfWork.doInTransactionWithRetry(() -> {
            InternalTableManager manager = InternalTableManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
            manager.updateInternalTable(TABLE_INDENTITY, copyForWrite -> copyForWrite.setRowCount(1L));
            return null;
        }, PROJECT);

        TransactionException exception = Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.updateInternalTable(PROJECT, tableName, db, partitionCols, dateFormat,
                        tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

        Assertions.assertEquals(String.format(Locale.ROOT, MsgPicker.getMsg().getInternalTableEmpty(),
                table.getDatabase() + "." + table.getName()), exception.getCause().getMessage());
    }

    @Test
    void testCreateInternalPathFailedErrorCode() throws Exception {
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        FileSystem mockFileSystem = mock(FileSystem.class);
        try (MockedStatic<HadoopUtil> mockedHadoopUtil = Mockito.mockStatic(HadoopUtil.class)) {
            mockedHadoopUtil.when(HadoopUtil::getWorkingFileSystem).thenReturn(mockFileSystem);
            doThrow(new IOException("Simulated IO error")).when(mockFileSystem).mkdirs(any(Path.class));
            KylinException exception = Assertions.assertThrows(KylinException.class, () -> {
                String path = "mocked/path/to/internal_table";
                internalTableService.createInternalTablePath(path);
            });
            Assertions.assertEquals(String.format(Locale.ROOT, MsgPicker.getMsg().getInternalTablePath()),
                    exception.getMessage());
        }
    }

    @Test
    void testLoadUnPartitionedTableErrorCode() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        internalTableService.createInternalTable(PROJECT, table, InternalTableDesc.StorageType.PARQUET.name());
        String startDate = "1325347200000"; // 2012-01-01
        String endDate = "1325865600000"; // 2012-01-07
        TransactionException exception = Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.loadIntoInternalTable(PROJECT, table.getName(), table.getDatabase(), true,
                        false, startDate, endDate, null));
        Assertions.assertEquals(String.format(Locale.ROOT, MsgPicker.getMsg().getInternalTableUnpartitioned()),
                exception.getCause().getMessage());
    }

    @Test
    void testInvalidInternalParamUnMatch() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        KylinException exception = Assertions.assertThrows(KylinException.class,
                () -> internalTableService.checkParameters(null, table, "yyyy-MM-dd"));
        Assertions.assertEquals(
                String.format(Locale.ROOT, MsgPicker.getMsg().getPartitionColumnNotExist(), table.getIdentity()),
                exception.getMessage());
    }

    @Test
    void testInternalDataFormatUnMatch() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        KylinException exception = Assertions.assertThrows(KylinException.class,
                () -> internalTableService.checkParameters(new String[] { "TRANS_ID", "CAL_DT" }, table, "yyyy-MM"));
        Assertions.assertEquals(String.format(Locale.ROOT, MsgPicker.getMsg().getIncorrectDateformat(), "yyyy-MM"),
                exception.getMessage());
    }

    @Test
    void testInternalTableShowDetailsFuzzy() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        boolean isFuzzy = true;
        boolean needDetails = true;
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertTrue(internalTableFolder.exists() && internalTableFolder.isDirectory());

        List<InternalTableDescResponse> tables = internalTableService.getTableList(PROJECT, isFuzzy, needDetails,
                table.getDatabase(), table.getName());
        Assertions.assertEquals(tables.get(0).getTableName(), table.getName());
        Assertions.assertEquals(DATE_COL, tables.get(0).getTimePartitionCol());
        List<ColumnDesc> tableColumn = tables.get(0).getColumns();
        ColumnDesc[] columns = internalTable.getColumns();
        Assertions.assertEquals(tableColumn.size(), columns.length);
        for (int i = 0; i < columns.length; i++) {
            ColumnDesc columninfo = tableColumn.get(i);
            Assertions.assertEquals(columns[i].getName(), columninfo.getName());
            Assertions.assertEquals(columns[i].getId(), columninfo.getId());
            Assertions.assertEquals(columns[i].getDatatype(), columninfo.getDatatype());
        }

        tables.clear();
        tables = internalTableService.getTableList(PROJECT, isFuzzy, needDetails, "E", "S");
        Assertions.assertEquals(tables.get(0).getTableName(), table.getName());
        Assertions.assertEquals(DATE_COL, tables.get(0).getTimePartitionCol());
        tableColumn = tables.get(0).getColumns();
        Assertions.assertEquals(tableColumn.size(), columns.length);

        KylinException notExistException = null;
        tables.clear();
        try {
            tables = internalTableService.getTableList(PROJECT, isFuzzy, needDetails, "DEFAULT2", "DUMMY");
        } catch (KylinException e) {
            notExistException = e;
        }
        Assertions.assertTrue(tables.isEmpty());
        Assertions.assertTrue(
                null != notExistException && notExistException.getErrorCode().getCodeString().equals("KE-010007014"));

        //Illegal string
        notExistException = null;
        try {
            tables = internalTableService.getTableList(PROJECT, isFuzzy, needDetails, "DEFAULT", "1");
        } catch (KylinException e) {
            notExistException = e;
        }
        Assertions.assertTrue(tables.isEmpty());
        Assertions.assertTrue(
                null != notExistException && notExistException.getErrorCode().getCodeString().equals("KE-010007014"));
    }

    @Test
    void testInternalTableShowDetails() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        boolean isFuzzy = false;
        boolean needDetails = true;
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertTrue(internalTableFolder.exists() && internalTableFolder.isDirectory());

        List<InternalTableDescResponse> tables = internalTableService.getTableList(PROJECT, isFuzzy, needDetails,
                table.getDatabase(), table.getName());
        Assertions.assertEquals(tables.get(0).getTableName(), table.getName());
        Assertions.assertEquals(DATE_COL, tables.get(0).getTimePartitionCol());
        List<ColumnDesc> tableColumn = tables.get(0).getColumns();
        ColumnDesc[] columns = internalTable.getColumns();
        Assertions.assertEquals(tableColumn.size(), columns.length);
        for (int i = 0; i < columns.length; i++) {
            ColumnDesc columninfo = tableColumn.get(i);
            Assertions.assertEquals(columns[i].getName(), columninfo.getName());
            Assertions.assertEquals(columns[i].getId(), columninfo.getId());
            Assertions.assertEquals(columns[i].getDatatype(), columninfo.getDatatype());
        }

        KylinException notExistException = null;
        tables.clear();
        try {
            tables = internalTableService.getTableList(PROJECT, isFuzzy, needDetails, "DEFAULT", "T");
        } catch (KylinException e) {
            notExistException = e;
        }
        Assertions.assertTrue(tables.isEmpty());
        Assertions.assertTrue(
                null != notExistException && notExistException.getErrorCode().getCodeString().equals("KE-010007014"));
    }

    @Test
    public void testGetInternalTableInfoByFuzzyKey() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());

        List<InternalTableDesc> tables = internalTableManager.getInternalTableDescInfos("", "", true);
        Assert.assertEquals(1, tables.size());
        tables = internalTableManager.getInternalTableDescInfos("", "", false);
        Assert.assertEquals(1, tables.size());
        tables = internalTableManager.getInternalTableDescInfos("D", "", false);
        Assert.assertEquals(0, tables.size());
        tables = internalTableManager.getInternalTableDescInfos("D", "T", true);
        Assert.assertEquals(1, tables.size());//DEFAULT.TEST_KYLIN_FACT
        tables = internalTableManager.getInternalTableDescInfos("D", "T", false);
        Assert.assertEquals(0, tables.size());
        tables = internalTableManager.getInternalTableDescInfos("", "TEST_KYLIN_FACT", false);
        Assert.assertEquals(1, tables.size());
        tables = internalTableManager.getInternalTableDescInfos("", "TEST", true);
        Assert.assertEquals(1, tables.size());
        tables = internalTableManager.getInternalTableDescInfos("D", "", true);
        Assert.assertEquals(1, tables.size());
    }

    @Test
    void testGetIceBergInternalTableMeta() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.ICEBERG.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertTrue(internalTableFolder.exists() && internalTableFolder.isDirectory());
        InternalTableLoadJob internalTableLoadJob = new InternalTableLoadJob();
        SparkSession ss = SparderEnv.getSparkSession();

        long count = internalTableLoadJob.getInternalTableCount(internalTable, ss);
        Assert.assertEquals(0, count);

        InternalTableLoader internalTableLoader = new InternalTableLoader();
        val partitionInfos = internalTableLoader.getPartitionInfos(ss, internalTable);
        Assert.assertEquals(0, partitionInfos.length);
    }

    @Test
    void testGetGlutenInternalTableMeta() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);

        InternalTableLoadJob internalTableLoadJob = new InternalTableLoadJob();
        SparkSession ss = SparderEnv.getSparkSession();

        // gluten internal table cannot be created directly in the ut env
        internalTable.setStorageType(InternalTableDesc.StorageType.GLUTEN.name());
        internalTableManager.saveOrUpdateInternalTable(internalTable);

        ClickhouseTable mockClickhouseTable = mock(ClickhouseTable.class);
        Dataset mockDataFrame = mock(Dataset.class);
        when(mockClickhouseTable.toDF()).thenReturn(mockDataFrame);
        when(mockDataFrame.count()).thenReturn(100L);
        SparkSession mockSparkSession = mock(SparkSession.class);
        when(mockClickhouseTable.toDF()).thenReturn(mockDataFrame);
        try {
            Mockito.mockStatic(ClickhouseTable.class);
            when(ClickhouseTable.forPath(mockSparkSession, internalTable.getLocation()))
                    .thenReturn(mockClickhouseTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long count2 = internalTableLoadJob.getInternalTableCount(internalTable, mockSparkSession);
        Assert.assertEquals(100, count2);

        InternalTableLoader internalTableLoader = new InternalTableLoader();
        val partitionInfos2 = internalTableLoader.getPartitionInfos(ss, internalTable);
        Assert.assertEquals(0, partitionInfos2.length);
    }

    @Test
    void testGetDeltaInternalTableMeta() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.DELTALAKE.name());

        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertTrue(internalTableFolder.exists() && internalTableFolder.isDirectory());
        InternalTableLoadJob internalTableLoadJob = new InternalTableLoadJob();
        SparkSession ss = SparderEnv.getSparkSession();

        long count = internalTableLoadJob.getInternalTableCount(internalTable, ss);
        Assert.assertEquals(0, count);

        InternalTableLoader internalTableLoader = new InternalTableLoader();
        val partitionInfos = internalTableLoader.getPartitionInfos(ss, internalTable);
        Assert.assertEquals(0, partitionInfos.length);
    }
}
