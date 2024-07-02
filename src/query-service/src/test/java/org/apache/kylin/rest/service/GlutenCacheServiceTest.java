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

import static org.apache.kylin.common.msg.Message.LOAD_GLUTEN_CACHE_EXECUTE_ERROR;
import static org.apache.kylin.metadata.model.SegmentStatusEnum.READY;
import static org.apache.kylin.metadata.model.SegmentStatusEnum.WARNING;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.msg.CnMessage;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.streaming.ReflectionUtils;
import org.apache.kylin.rest.model.GlutenCacheExecuteResult;
import org.apache.kylin.rest.request.IndexGlutenCacheRequest;
import org.apache.kylin.rest.request.InternalTableGlutenCacheRequest;
import org.apache.kylin.utils.GlutenCacheUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.mock.web.MockHttpServletRequest;

import lombok.val;
import lombok.var;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
@PrepareForTest({ SparderEnv.class, GlutenCacheUtils.class })
public class GlutenCacheServiceTest extends NLocalFileMetadataTestCase {
    @InjectMocks
    private GlutenCacheService glutenCacheService = Mockito.spy(GlutenCacheService.class);
    @Mock
    private RouteService routeService = Mockito.mock(RouteService.class);
    @Mock
    private ModelService modelService = Mockito.mock(ModelService.class);
    @Mock
    private SparkSession sparkSession = Mockito.mock(SparkSession.class);
    @Mock
    private Dataset dataset = Mockito.mock(Dataset.class);
    @Mock
    private GenericRowWithSchema row = Mockito.mock(GenericRowWithSchema.class);
    @Mock
    private Segments segments = Mockito.mock(Segments.class);
    @Mock
    private Appender appender = Mockito.mock(Appender.class);

    private static final String COMMAND_0 = "test command 0";
    private static final String COMMAND_1 = "test command 1";
    private static final String COMMAND_2 = "test command 2";
    private static final String COMMAND_3 = "test command 3";
    private static final String COMMAND_4 = "test command 4";
    private static final String COMMAND_5 = "test command 5";
    private static final String PROJECT = "default";
    private static final String DATABASE = "databases";
    private static final String TABLE = "table";
    private static final String START = "";
    private static final List<String> COLUMNS = Lists.newArrayList();
    private static final String DATABASE_TABLE = StringUtils.upperCase(DATABASE + "." + TABLE);
    private static final String MODEL = "model";
    private static final String MODEL_ID = "modelId";
    private static final List<Long> INDEXES = Lists.newArrayList(1L, 100001L);
    private static final String GENERATE_CACHE_TABLE_COMMAND = "generateCacheTableCommand";
    private static final Set<String> GENERATE_MODEL_CACHE_COMMANDS_BY_SEGMENTS = Sets
            .newHashSet("generateModelCacheCommandsBySegments1", "generateModelCacheCommandsBySegments2");
    private static final Set<String> GENERATE_MODEL_CACHE_COMMANDS_BY_INDEXES = Sets
            .newHashSet("generateModelCacheCommandsByIndexes1", "generateModelCacheCommandsByIndexes2");
    private KylinConfig config;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        config = getTestConfig();
        config.setProperty("kylin.storage.columnar.spark-conf.spark.gluten.enabled", KylinConfig.TRUE);
        config.setProperty("kylin.storage.columnar.spark-conf.spark.plugins", "GlutenPlugin");
        config.setProperty("kylin.internal-table-enabled", KylinConfig.TRUE);
        PowerMockito.mockStatic(SparderEnv.class, GlutenCacheUtils.class);

        PowerMockito
                .when(GlutenCacheUtils.generateCacheTableCommand(config, PROJECT, DATABASE_TABLE, START, COLUMNS, true))
                .thenAnswer(invocation -> GENERATE_CACHE_TABLE_COMMAND);
        PowerMockito.when(GlutenCacheUtils.generateModelCacheCommandsBySegments(config, PROJECT, MODEL_ID, segments))
                .thenAnswer(invocation -> GENERATE_MODEL_CACHE_COMMANDS_BY_SEGMENTS);
        PowerMockito.when(GlutenCacheUtils.generateModelCacheCommandsByIndexes(config, PROJECT, MODEL_ID, INDEXES))
                .thenAnswer(invocation -> GENERATE_MODEL_CACHE_COMMANDS_BY_INDEXES);

        PowerMockito.when(SparderEnv.getSparkSession()).thenAnswer(invocation -> sparkSession);
        Mockito.when(sparkSession.sql(COMMAND_0)).thenReturn(dataset);
        Mockito.when(dataset.collectAsList()).thenReturn(Lists.newArrayList(row));
        Mockito.when(row.getBoolean(0)).thenReturn(true);
        Mockito.when(row.getString(1)).thenReturn("test row exception");

        ReflectionUtils.setField(glutenCacheService, "routeService", routeService);
        ReflectionUtils.setField(glutenCacheService, "modelService", modelService);

        Mockito.when(appender.getName()).thenReturn("mocked");
        Mockito.when(appender.isStarted()).thenReturn(true);
        ((Logger) LogManager.getRootLogger()).addAppender(appender);
    }

    @After
    public void after() {
        ((Logger) LogManager.getRootLogger()).removeAppender(appender);
        cleanupTestMetadata();
    }

    @Test
    public void glutenCacheAsync() {
        val commands = Lists.newArrayList(COMMAND_0);
        glutenCacheService.glutenCacheAsync(commands);
        ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var emptyLogs = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals(GlutenCacheService.class.getName()))
                .filter(event -> event.getLevel().equals(Level.ERROR))
                .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEmpty(emptyLogs));

        val message = "test exception";
        Mockito.when(sparkSession.sql(anyString())).thenThrow(new KylinRuntimeException(message));
        glutenCacheService.glutenCacheAsync(commands);
        logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var log = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals(GlutenCacheService.class.getName()))
                .filter(event -> event.getLevel().equals(Level.ERROR))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst().orElseThrow(AssertionError::new);
        Assert.assertTrue(StringUtils.contains(log, message));

        val message2 = "test static exception";
        PowerMockito.when(SparderEnv.getSparkSession()).thenThrow(new KylinRuntimeException(message2));
        glutenCacheService.glutenCacheAsync(commands);
        logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        val logs = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals(GlutenCacheService.class.getName()))
                .filter(event -> event.getLevel().equals(Level.ERROR))
                .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
        Assert.assertTrue(logs.stream().anyMatch(msg -> StringUtils.contains(msg, message2)));
    }

    @Test
    public void glutenCacheSparderError() {
        var glutenCacheResponse = glutenCacheService.glutenCache(Lists.newArrayList());
        Assert.assertTrue(glutenCacheResponse.getResult());
        var executeResults = glutenCacheResponse.getExecuteResults();
        Assert.assertEquals(0, executeResults.size());
        Assert.assertTrue(StringUtils.isBlank(glutenCacheResponse.getExceptionMessage()));

        val message = "test static exception";
        PowerMockito.when(SparderEnv.getSparkSession()).thenThrow(new KylinRuntimeException(message));
        glutenCacheResponse = glutenCacheService.glutenCache(Lists.newArrayList(COMMAND_0));
        Assert.assertFalse(glutenCacheResponse.getResult());
        executeResults = glutenCacheResponse.getExecuteResults();
        Assert.assertEquals(0, executeResults.size());
        Assert.assertEquals(message, glutenCacheResponse.getExceptionMessage());
    }

    @Test
    public void glutenCache() {
        val message1 = "test COMMAND_1 exception";
        Mockito.when(sparkSession.sql(COMMAND_1)).thenThrow(new KylinRuntimeException(message1));

        val row2 = Mockito.mock(GenericRowWithSchema.class);
        val dataset2 = Mockito.mock(Dataset.class);
        Mockito.when(sparkSession.sql(COMMAND_2)).thenReturn(dataset2);
        Mockito.when(dataset2.collectAsList()).thenReturn(Lists.newArrayList(row2));
        Mockito.when(row2.getBoolean(0)).thenReturn(false);
        Mockito.when(row2.getString(1)).thenReturn("");

        val message3 = "test COMMAND_3 exception";
        val row3 = Mockito.mock(GenericRowWithSchema.class);
        val dataset3 = Mockito.mock(Dataset.class);
        Mockito.when(sparkSession.sql(COMMAND_3)).thenReturn(dataset3);
        Mockito.when(dataset3.collectAsList()).thenReturn(Lists.newArrayList(row3));
        Mockito.when(row3.getBoolean(0)).thenReturn(false);
        Mockito.when(row3.getString(1)).thenReturn(message3);

        val dataset4 = Mockito.mock(Dataset.class);
        Mockito.when(sparkSession.sql(COMMAND_4)).thenReturn(dataset4);
        Mockito.when(dataset4.collectAsList()).thenReturn(Lists.newArrayList());

        val message5 = "test COMMAND_5 exception";
        Mockito.when(sparkSession.sql(COMMAND_5)).thenThrow(new KylinRuntimeException(message5));

        val commands = Lists.newArrayList(COMMAND_0, COMMAND_1, COMMAND_2, COMMAND_3, COMMAND_4, COMMAND_5);
        var glutenCacheResponse = glutenCacheService.glutenCache(commands);
        Assert.assertFalse(glutenCacheResponse.getResult());
        var executeResults = glutenCacheResponse.getExecuteResults();
        Assert.assertEquals(6, executeResults.size());

        checkExecuteResult(executeResults, message1, message3, message5);
    }

    private static void checkExecuteResult(List<GlutenCacheExecuteResult> executeResults, String message1,
            String message3, String message5) {
        var result0 = executeResults.get(0);
        Assert.assertTrue(result0.getResult());

        var result1 = executeResults.get(1);
        Assert.assertFalse(result1.getResult());
        Assert.assertEquals(message1, result1.getReason());

        var result2 = executeResults.get(2);
        Assert.assertFalse(result2.getResult());
        Assert.assertEquals(String.format(Locale.ROOT, LOAD_GLUTEN_CACHE_EXECUTE_ERROR, "Result exception is empty!!"),
                result2.getReason());

        var result3 = executeResults.get(3);
        Assert.assertFalse(result3.getResult());
        Assert.assertEquals(message3, result3.getReason());

        var result4 = executeResults.get(4);
        Assert.assertFalse(result4.getResult());
        Assert.assertEquals(String.format(Locale.ROOT, LOAD_GLUTEN_CACHE_EXECUTE_ERROR, "Result rows is empty!!"),
                result4.getReason());

        var result5 = executeResults.get(5);
        Assert.assertFalse(result5.getResult());
        Assert.assertEquals(message5, result5.getReason());
    }

    @Test
    public void internalTableGlutenCache() throws Exception {
        val servletRequest = new MockHttpServletRequest();
        val request = new InternalTableGlutenCacheRequest();
        request.setProject(PROJECT);
        request.setDatabase(DATABASE);
        request.setTable(TABLE);
        request.setStart(START);
        request.setColumns(COLUMNS);
        glutenCacheService.internalTableGlutenCache(request, servletRequest);
        ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var log = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals(GlutenCacheService.class.getName()))
                .filter(event -> event.getLevel().equals(Level.INFO))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst().orElseThrow(AssertionError::new);
        Assert.assertEquals(String.format(Locale.ROOT, "InternalTable[%s] cache command is [%s]", DATABASE_TABLE,
                GENERATE_CACHE_TABLE_COMMAND), log);
    }

    @Test
    public void indexGlutenCacheV1() throws Exception {
        val servletRequest = new MockHttpServletRequest();
        val request = new IndexGlutenCacheRequest();
        request.setModel(MODEL);
        request.setProject(PROJECT);

        val model = Mockito.mock(NDataModel.class);
        Mockito.when(modelService.getModel(MODEL, PROJECT)).thenReturn(model);
        Mockito.when(model.getStorageType()).thenReturn(NDataModel.DataStorageType.V1);
        Mockito.when(model.getId()).thenReturn(MODEL_ID);
        Mockito.when(modelService.getSegmentsByRange(MODEL_ID, PROJECT, request.getStart(), request.getEnd()))
                .thenReturn(segments);
        Mockito.when(segments.getSegments(READY, WARNING)).thenReturn(segments);

        glutenCacheService.indexGlutenCache(request, servletRequest);
        ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var log = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals(GlutenCacheService.class.getName()))
                .filter(event -> event.getLevel().equals(Level.INFO))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst().orElseThrow(AssertionError::new);
        Assert.assertEquals(String.format(Locale.ROOT, "Model[%s] cache command is [%s]", MODEL,
                GENERATE_MODEL_CACHE_COMMANDS_BY_SEGMENTS), log);

        try {
            request.setIndexes(INDEXES);
            glutenCacheService.indexGlutenCache(request, servletRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinRuntimeException);
            Assert.assertEquals(
                    String.format(Locale.ROOT, "Model[%s] StorageType is V1, indexes need null or empty", MODEL),
                    e.getMessage());
        }
    }

    @Test
    public void indexGlutenCacheV3() throws Exception {
        val servletRequest = new MockHttpServletRequest();
        val request = new IndexGlutenCacheRequest();
        request.setModel(MODEL);
        request.setProject(PROJECT);
        request.setIndexes(INDEXES);

        val model = Mockito.mock(NDataModel.class);
        Mockito.when(modelService.getModel(MODEL, PROJECT)).thenReturn(model);
        Mockito.when(model.getStorageType()).thenReturn(NDataModel.DataStorageType.DELTA);
        Mockito.when(model.getId()).thenReturn(MODEL_ID);

        glutenCacheService.indexGlutenCache(request, servletRequest);
        ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var log = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals(GlutenCacheService.class.getName()))
                .filter(event -> event.getLevel().equals(Level.INFO))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst().orElseThrow(AssertionError::new);
        Assert.assertEquals(String.format(Locale.ROOT, "Model[%s] cache command is [%s]", MODEL,
                GENERATE_MODEL_CACHE_COMMANDS_BY_INDEXES), log);

        try {
            request.setStart("start");
            request.setEnd("");
            glutenCacheService.indexGlutenCache(request, servletRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinRuntimeException);
            Assert.assertEquals(String.format(Locale.ROOT, "Model[%s] StorageType is V3, start/end need null", MODEL),
                    e.getMessage());
        }
        try {
            request.setStart("");
            request.setEnd("end");
            glutenCacheService.indexGlutenCache(request, servletRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinRuntimeException);
            Assert.assertEquals(String.format(Locale.ROOT, "Model[%s] StorageType is V3, start/end need null", MODEL),
                    e.getMessage());
        }
        try {
            request.setStart("start");
            request.setEnd("end");
            glutenCacheService.indexGlutenCache(request, servletRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinRuntimeException);
            Assert.assertEquals(String.format(Locale.ROOT, "Model[%s] StorageType is V3, start/end need null", MODEL),
                    e.getMessage());
        }
    }

    @Test
    public void indexGlutenCacheDisabled() throws Exception {
        val servletRequest = new MockHttpServletRequest();
        val request = new IndexGlutenCacheRequest();
        request.setModel(MODEL);
        request.setProject(PROJECT);
        request.setIndexes(INDEXES);

        config.setProperty("kylin.storage.columnar.spark-conf.spark.gluten.enabled", KylinConfig.FALSE);
        try {
            MsgPicker.setMsg("cn");
            glutenCacheService.indexGlutenCache(request, servletRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(CnMessage.getInstance().getGlutenDisabled(), e.getMessage());
        }
        try {
            MsgPicker.setMsg("en");
            glutenCacheService.indexGlutenCache(request, servletRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(Message.getInstance().getGlutenDisabled(), e.getMessage());
        }
        config.setProperty("kylin.storage.columnar.spark-conf.spark.gluten.enabled", KylinConfig.TRUE);
    }

    @Test
    public void internalTableGlutenCacheDisabled() throws Exception {
        val servletRequest = new MockHttpServletRequest();
        val request = new InternalTableGlutenCacheRequest();
        request.setProject(PROJECT);
        request.setDatabase(DATABASE);
        request.setTable(TABLE);
        request.setStart(START);
        request.setColumns(COLUMNS);

        {
            config.setProperty("kylin.storage.columnar.spark-conf.spark.gluten.enabled", KylinConfig.FALSE);
            try {
                MsgPicker.setMsg("cn");
                glutenCacheService.internalTableGlutenCache(request, servletRequest);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals(CnMessage.getInstance().getGlutenDisabled(), e.getMessage());
            }
            try {
                MsgPicker.setMsg("en");
                glutenCacheService.internalTableGlutenCache(request, servletRequest);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals(Message.getInstance().getGlutenDisabled(), e.getMessage());
            }
            config.setProperty("kylin.storage.columnar.spark-conf.spark.gluten.enabled", KylinConfig.TRUE);
        }

        {
            val projectConfig = NProjectManager.getProjectConfig(PROJECT);
            projectConfig.setProperty("kylin.internal-table-enabled", KylinConfig.FALSE);
            try {
                MsgPicker.setMsg("cn");
                glutenCacheService.internalTableGlutenCache(request, servletRequest);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals(CnMessage.getInstance().getInternalTableDisabled(), e.getMessage());
            }
            try {
                MsgPicker.setMsg("en");
                glutenCacheService.internalTableGlutenCache(request, servletRequest);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals(Message.getInstance().getInternalTableDisabled(), e.getMessage());
            }
            projectConfig.setProperty("kylin.internal-table-enabled", KylinConfig.TRUE);
        }
    }
}
