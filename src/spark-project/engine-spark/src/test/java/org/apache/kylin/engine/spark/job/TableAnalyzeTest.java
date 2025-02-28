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

package org.apache.kylin.engine.spark.job;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.GlutenDisabled;
import org.apache.kylin.GlutenRunner;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.utils.SparkConfHelper;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;

@RunWith(GlutenRunner.class)
public class TableAnalyzeTest extends NLocalWithSparkSessionTestBase {

    private NTableMetadataManager tableMgr;
    @Mock
    private Appender appender = Mockito.mock(Appender.class);

    @Before
    public void setup() {
        tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf.set(SparkConfHelper.EXECUTOR_INSTANCES, "1");
        sparkConf.set(SparkConfHelper.EXECUTOR_CORES, "1");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

        Mockito.when(appender.getName()).thenReturn("mocked");
        Mockito.when(appender.isStarted()).thenReturn(true);
        ((Logger) LogManager.getRootLogger()).addAppender(appender);
    }

    @After
    public void after() {
        ((Logger) LogManager.getRootLogger()).removeAppender(appender);
    }

    @Test
    public void testSampleFullTable() {
        TableDesc tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        new TableAnalyzeJob().analyzeTable(tableDesc, getProject(), 20_000_000, ss);
        val tableExt = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertEquals(10, tableExt.getSampleRows().size());

        {
            var result = tableExt.getColumnStatsByName("TRANS_ID");
            Assert.assertEquals(0, result.getNullCount());
            Assert.assertEquals(9571, result.getCardinality());
            Assert.assertEquals("9999", result.getMaxValue());
            Assert.assertEquals("0", result.getMinValue());
        }

        {
            val result = tableExt.getColumnStatsByName("CAL_DT");
            Assert.assertEquals(0, result.getNullCount());
            Assert.assertEquals(722, result.getCardinality());
            Assert.assertEquals("2014-01-01", result.getMaxValue());
            Assert.assertEquals("2012-01-01", result.getMinValue());
        }

        {
            val result = tableExt.getColumnStatsByName("LSTG_FORMAT_NAME");
            Assert.assertEquals(0, result.getNullCount());
            Assert.assertEquals(5, result.getCardinality());
            Assert.assertEquals("Others", result.getMaxValue());
            Assert.assertEquals("ABIN", result.getMinValue());
        }

        {
            val result = tableExt.getColumnStatsByName("PRICE");
            Assert.assertEquals(0, result.getNullCount());
            Assert.assertEquals(8787, result.getCardinality());
            Assert.assertEquals("999.8400", result.getMaxValue());
            Assert.assertEquals("-99.7900", result.getMinValue());
        }
    }

    @Test
    @GlutenDisabled("max(col) with null data gluten returns NaN, but spark return null")
    public void testSampleTableForColumnOrRowAlwaysNull() {
        // case 1: this case test specified column always null, corresponding column is 'CATEG_BUSN_MGR'
        TableDesc testCategoryGroupings = tableMgr.getTableDesc("DEFAULT.TEST_CATEGORY_GROUPINGS");
        final ColumnDesc categBusnMgr = Arrays.stream(testCategoryGroupings.getColumns())
                .filter(columnDesc -> columnDesc.getName().equalsIgnoreCase("CATEG_BUSN_MGR"))
                .collect(Collectors.toList()).get(0);
        new TableAnalyzeJob().analyzeTable(testCategoryGroupings, getProject(), 10000, ss);
        val tableExt = tableMgr.getTableExtIfExists(testCategoryGroupings);
        final TableExtDesc.ColumnStats columnStats = tableExt.getColumnStatsByName("CATEG_BUSN_MGR");
        Assert.assertEquals(categBusnMgr.getName(), columnStats.getColumnName());
        Assert.assertNull(columnStats.getMaxValue());
        Assert.assertNull(columnStats.getMinValue());
        Assert.assertEquals(144, columnStats.getNullCount());
        Assert.assertEquals(0, columnStats.getCardinality());

        // case 2: this case test sample data has a line always null in each column
        TableDesc testEncodings = tableMgr.getTableDesc("DEFAULT.TEST_ENCODING");
        new TableAnalyzeJob().analyzeTable(testEncodings, getProject(), 10000, ss);
        final TableExtDesc testEncodingsExt = tableMgr.getTableExtIfExists(testEncodings);
        final List<String[]> sampleRows = testEncodingsExt.getSampleRows();
        final String[] rowValue = sampleRows.get(sampleRows.size() - 1);
        Arrays.stream(rowValue).forEach(Assert::assertNull);

        // case 3: this case test sample data with a large long string value
        TableDesc allMeasTbl = tableMgr.getTableDesc("DEFAULT.TEST_MEASURE");
        new TableAnalyzeJob().analyzeTable(allMeasTbl, getProject(), 10000, ss);
        final TableExtDesc allMeasTblExt = tableMgr.getTableExtIfExists(allMeasTbl);
        Assert.assertNotNull(allMeasTblExt);
        String minName1 = allMeasTblExt.getColumnStatsByName("NAME1").getMinValue();
        Assert.assertNotNull(minName1);
        Assert.assertTrue(minName1.length() > 256);

    }

    @Test
    public void testSamplingTaskRunningFailedForTableNotExistAnyMore() {
        String tableIdentity = "DEFAULT.NOT_EXIST_TABLE";
        Assert.assertNull(tableMgr.getTableDesc(tableIdentity));

        // mock a table desc without corresponding data exist
        final TableDesc tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_COUNTRY");
        final TableDesc notExistTableDesc = tableMgr.copyForWrite(tableDesc);
        notExistTableDesc.setName(tableIdentity);

        try {
            new TableAnalyzeJob().analyzeTable(notExistTableDesc, getProject(), 10000, ss);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof AnalysisException);
            Assert.assertTrue(e.getMessage().startsWith("Path does not exist:")
                    && e.getMessage().endsWith("/data/DEFAULT.NOT_EXIST_TABLE.csv"));
        }
    }

    @Test
    public void testSamplePartTable() {
        TableDesc tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        new TableAnalyzeJob().analyzeTable(tableDesc, getProject(), 100, ss);
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val tableExt = tableMetadataManager.getTableExtIfExists(tableDesc);
        Assert.assertEquals(10, tableExt.getSampleRows().size());
        Assert.assertEquals(100.0 / 10000, tableExt.getTotalRows() / 10000.0, 0.1);

    }

    @Test
    public void testJdbcTableSkipCalculateViewMetas() {
        val tableDesc = Mockito.mock(TableDesc.class);
        Mockito.when(tableDesc.getSourceType()).thenReturn(ISourceAware.ID_JDBC);
        Mockito.when(tableDesc.getBackTickIdentity()).thenReturn("UT.TEST");

        val tableAnalyzeExec = new TableAnalyzeExec(tableDesc, getProject(), 100, ss, RandomUtil.randomUUIDStr());
        tableAnalyzeExec.calculateViewMetasIfNeeded(tableDesc);

        ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var log = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals(TableAnalyzeExec.class.getName()))
                .filter(event -> event.getLevel().equals(Level.INFO))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst().orElseThrow(AssertionError::new);
        Assert.assertEquals("Table [UT.TEST] sourceType is JDBC, skip to calculate view meta", log);
    }
}
