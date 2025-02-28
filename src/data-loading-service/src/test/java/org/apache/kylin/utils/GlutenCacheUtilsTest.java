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

package org.apache.kylin.utils;

import static org.apache.kylin.common.constant.Constants.ASYNC;
import static org.apache.kylin.common.constant.Constants.CACHE_MODEL_COMMAND;
import static org.apache.kylin.common.constant.Constants.FILTER_COMMAND;
import static org.apache.kylin.common.constant.Constants.START;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayoutDetailsManager;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.streaming.ReflectionUtils;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.InternalTableLoadingService;
import org.apache.kylin.rest.service.InternalTableService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.val;
import lombok.var;

@MetadataInfo
class GlutenCacheUtilsTest extends AbstractTestCase {
    static final String PROJECT = "default";
    static final String TABLE_INDENTITY = "DEFAULT.TEST_KYLIN_FACT";
    static final String DATE_COL = "CAL_DT";

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Spy
    private InternalTableLoadingService internalTableLoadingService = Mockito.spy(new InternalTableLoadingService());
    @InjectMocks
    private InternalTableService internalTableService = Mockito.spy(new InternalTableService());

    @InjectMocks
    private TableService tableService = mock(TableService.class);

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
        MockitoAnnotations.openMocks(this);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        SparkJobFactoryUtils.initJobFactory();
        overwriteSystemProp("kylin.source.provider.9", "org.apache.kylin.engine.spark.mockup.CsvSource");
        ReflectionUtils.setField(internalTableService, "aclEvaluate", aclEvaluate);
        ReflectionUtils.setField(internalTableService, "internalTableLoadingService", internalTableLoadingService);
    }

    @Test
    void generateCacheTableCommand() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        val datePartitionFormat = "yyyy-MM-dd";
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn(datePartitionFormat);
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);
        val location = internalTable.getLocation();
        Assertions.assertTrue(StringUtils.isNotBlank(location));

        generateTableCommand(config, location, internalTable, datePartitionFormat);
        generateTableCommandError(config);
    }

    private static void generateTableCommand(KylinConfig config, String location, InternalTableDesc internalTable,
            String datePartitionFormat) {
        var start = "";
        List<String> columns = Lists.newArrayList();
        var result = GlutenCacheUtils.generateCacheTableCommand(config, PROJECT, TABLE_INDENTITY, start, columns,
                false);
        var expected = String.format(Locale.ROOT, "CACHE DATA %s SELECT %s FROM '%s' %s", StringUtils.EMPTY, START,
                location, StringUtils.EMPTY);
        Assertions.assertEquals(expected, result);

        columns = Arrays.stream(internalTable.getColumns()).map(ColumnDesc::getName).collect(Collectors.toList());
        result = GlutenCacheUtils.generateCacheTableCommand(config, PROJECT, TABLE_INDENTITY, start, columns, false);
        expected = String.format(Locale.ROOT, "CACHE DATA %s SELECT %s FROM '%s' %s", StringUtils.EMPTY,
                String.join(",", columns), location, StringUtils.EMPTY);
        Assertions.assertEquals(expected, result);

        start = "883584000000";
        result = GlutenCacheUtils.generateCacheTableCommand(config, PROJECT, TABLE_INDENTITY, start, columns, false);
        val formatStart = DateFormat.formatToTimeStr(Long.parseLong(start), datePartitionFormat);
        val filterCommand = String.format(Locale.ROOT, FILTER_COMMAND, DATE_COL, formatStart);
        expected = String.format(Locale.ROOT, "CACHE DATA %s SELECT %s FROM '%s' %s", StringUtils.EMPTY,
                String.join(",", columns), location, filterCommand);
        Assertions.assertEquals(expected, result);
    }

    private static void generateTableCommandError(KylinConfig config) {
        var start = "";
        List<String> columns = Lists.newArrayList("test_123");
        try {
            GlutenCacheUtils.generateCacheTableCommand(config, PROJECT, TABLE_INDENTITY + 1, start, columns, false);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertInstanceOf(KylinRuntimeException.class, e);
            Assertions.assertEquals(String.format(Locale.ROOT, "InternalTable [%s] not exist", TABLE_INDENTITY + 1),
                    e.getMessage());
        }
        try {
            GlutenCacheUtils.generateCacheTableCommand(config, PROJECT, TABLE_INDENTITY, start, columns, false);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertInstanceOf(KylinRuntimeException.class, e);
            Assertions.assertEquals(
                    String.format(Locale.ROOT, "InternalTable[%s] column[%s] not exist", TABLE_INDENTITY, columns),
                    e.getMessage());
        }

    }

    @Test
    void generateV1ModelCacheCommands() {
        val config = TestUtils.getTestConfig();
        val dfId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val dfManager = NDataflowManager.getInstance(config, PROJECT);
        val df = dfManager.getDataflow(dfId);
        val segments = df.getSegments();
        {
            val segmentIds = segments.stream().map(RootPersistentEntity::getId).collect(Collectors.toList());
            val expected = segments.stream().map(seg -> df.getSegmentHdfsPath(seg.getId()))
                    .map(path -> String.format(Locale.ROOT, CACHE_MODEL_COMMAND, StringUtils.EMPTY, path))
                    .collect(Collectors.toSet());
            val result = GlutenCacheUtils.generateModelCacheCommands(config, PROJECT, dfId, segmentIds);
            Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(expected, result)));
            Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(result, expected)));
        }

        {
            val expected = segments.stream().map(seg -> df.getSegmentHdfsPath(seg.getId()))
                    .map(path -> String.format(Locale.ROOT, CACHE_MODEL_COMMAND, ASYNC, path))
                    .collect(Collectors.toSet());
            val result = GlutenCacheUtils.generateModelCacheCommandsBySegments(config, PROJECT, dfId, segments);
            Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(expected, result)));
            Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(result, expected)));
        }
    }

    @Test
    void generateModelV3CacheCommands() {
        val config = TestUtils.getTestConfig();
        val project = "storage_v3_test";
        val dfId = "7d840904-7b34-4edd-aabd-79df992ef32e";
        val dfManager = NDataflowManager.getInstance(config, project);
        val df = dfManager.getDataflow(dfId);
        val segments = df.getSegments();
        val layoutDetailsManager = NDataLayoutDetailsManager.getInstance(config, project);
        val segmentIds = segments.stream().map(RootPersistentEntity::getId).collect(Collectors.toList());
        val expected = df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .map(id -> layoutDetailsManager.getNDataLayoutDetails(dfId, id).getLocation())
                .map(path -> String.format(Locale.ROOT, CACHE_MODEL_COMMAND, StringUtils.EMPTY, path))
                .collect(Collectors.toSet());
        val result = GlutenCacheUtils.generateModelCacheCommands(config, project, dfId, segmentIds);
        Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(expected, result)));
        Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(result, expected)));
    }

    @Test
    void generateModelCacheCommandsByIndexes() {
        val config = TestUtils.getTestConfig();
        val project = "storage_v3_test";
        val dfId = "7d840904-7b34-4edd-aabd-79df992ef32e";
        val dfManager = NDataflowManager.getInstance(config, project);
        val df = dfManager.getDataflow(dfId);
        val layoutDetailsManager = NDataLayoutDetailsManager.getInstance(config, project);

        var indexIds = df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList());
        var expected = indexIds.stream().map(id -> layoutDetailsManager.getNDataLayoutDetails(dfId, id).getLocation())
                .map(path -> String.format(Locale.ROOT, CACHE_MODEL_COMMAND, ASYNC, path)).collect(Collectors.toSet());
        var result = GlutenCacheUtils.generateModelCacheCommandsByIndexes(config, project, dfId, indexIds);
        Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(expected, result)));
        Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(result, expected)));

        result = GlutenCacheUtils.generateModelCacheCommandsByIndexes(config, project, dfId, Lists.newArrayList());
        Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(expected, result)));
        Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(result, expected)));

        indexIds.remove(0);
        expected = indexIds.stream().map(id -> layoutDetailsManager.getNDataLayoutDetails(dfId, id).getLocation())
                .map(path -> String.format(Locale.ROOT, CACHE_MODEL_COMMAND, ASYNC, path)).collect(Collectors.toSet());
        result = GlutenCacheUtils.generateModelCacheCommandsByIndexes(config, project, dfId, indexIds);
        Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(expected, result)));
        Assertions.assertTrue(CollectionUtils.isEmpty(Sets.difference(result, expected)));
    }

    @Test
    void generateV3ModelCacheCommandsError() {
        val config = TestUtils.getTestConfig();
        val project = "storage_v3_test";
        val dfId = "7d840904-7b34-4edd-aabd-79df992ef32e";
        val dfManager = NDataflowManager.getInstance(config, project);
        val df = dfManager.getDataflow(dfId);
        val indexIds = df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList());

        try {
            indexIds.add(0L);
            GlutenCacheUtils.generateModelCacheCommandsByIndexes(config, project, dfId, indexIds);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertInstanceOf(KylinRuntimeException.class, e);
            Assertions.assertEquals(
                    String.format(Locale.ROOT, "Model[%s] indexes[%s] not exist", dfId, Sets.newHashSet(0L)),
                    e.getMessage());
        }
    }

    @Test
    void generateModelCacheCommandsStorageTypeError() {
        val config = TestUtils.getTestConfig();
        val dfId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val dfManager = NDataflowManager.getInstance(config, PROJECT);
        val df = dfManager.getDataflow(dfId);
        val segments = df.getSegments();
        try (MockedStatic<NDataModelManager> modelManagerMockedStatic = Mockito.mockStatic(NDataModelManager.class)) {
            val modelManager = Mockito.mock(NDataModelManager.class);
            val model = Mockito.mock(NDataModel.class);
            modelManagerMockedStatic.when(() -> NDataModelManager.getInstance(config, PROJECT))
                    .thenReturn(modelManager);
            Mockito.when(modelManager.getDataModelDesc(dfId)).thenReturn(model);
            Mockito.when(model.getStorageType()).thenReturn(NDataModel.DataStorageType.ICEBERG);
            try {
                GlutenCacheUtils.generateModelCacheCommandsBySegments(config, PROJECT, dfId, segments);
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertInstanceOf(KylinRuntimeException.class, e);
                Assertions.assertEquals(
                        String.format(Locale.ROOT, "Model StorageType[%s] not support load gluten cache by segments",
                                NDataModel.DataStorageType.ICEBERG),
                        e.getMessage());
            }
            try {
                val segmentIds = segments.stream().map(RootPersistentEntity::getId).collect(Collectors.toList());
                GlutenCacheUtils.generateModelCacheCommands(config, PROJECT, dfId, segmentIds);
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertInstanceOf(KylinRuntimeException.class, e);
                Assertions.assertEquals(String.format(Locale.ROOT,
                        "Model StorageType[%s] not support load gluten cache", NDataModel.DataStorageType.ICEBERG),
                        e.getMessage());
            }

            try {
                GlutenCacheUtils.generateModelCacheCommandsByIndexes(config, PROJECT, dfId, Lists.newArrayList());
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertInstanceOf(KylinRuntimeException.class, e);
                Assertions.assertEquals(
                        String.format(Locale.ROOT, "Model StorageType[%s] not support load gluten cache by indexes",
                                NDataModel.DataStorageType.ICEBERG),
                        e.getMessage());
            }
        }
    }
}
