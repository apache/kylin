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

package org.apache.kylin.metadata.cube.model;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import lombok.val;

@RunWith(TimeZoneTestRunner.class)
public class NDataLoadingRangeManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NDataLoadingRangeManager dataLoadingRangeManager;
    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetInstance() {
        NDataLoadingRangeManager mgrSsb = NDataLoadingRangeManager.getInstance(getTestConfig(), "ssb");
        Assert.assertNotEquals(DEFAULT_PROJECT, mgrSsb);

    }

    @Test
    public void testAppendSegRangeErrorCase() {
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        // test error case, add a segRange with has the overlaps/gap
        long start = 1536813121000L;
        long end = 1536813191000L;
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start, end);
        savedDataLoadingRange = dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range);
        start = 0L;
        end = 1005277100000L;
        SegmentRange.TimePartitionedSegmentRange range1 = new SegmentRange.TimePartitionedSegmentRange(start, end);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("has overlaps/gap with existing segmentRanges");
        dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range1);
    }

    @Test
    public void testCreateAndUpdateDataLoadingRange() {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
        Assert.assertEquals(DEFAULT_PROJECT, savedDataLoadingRange.getProject());
    }

    @Test
    public void testCreateDataLoadingRange_StringColumn() {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.LSTG_FORMAT_NAME";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setPartitionDateFormat("YYYY");
        dataLoadingRange.setColumnName(columnName);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        Assert.assertEquals(DEFAULT_PROJECT, savedDataLoadingRange.getProject());
        Assert.assertEquals(savedDataLoadingRange.getColumnName(), columnName);
    }

    @Test
    public void testCreateDataLoadingRange_IntegerColumn() {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.LEAF_CATEG_ID";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setPartitionDateFormat("YYYY");
        dataLoadingRange.setColumnName(columnName);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
        Assert.assertEquals(DEFAULT_PROJECT, savedDataLoadingRange.getProject());
        Assert.assertEquals(savedDataLoadingRange.getColumnName(), columnName);
    }

    @Test
    public void testGetSegRangeToBuildForNewDataflow_MonthAndWeek() {
        //2012/12/25-2013/01/15
        String start = "2012-12-25 14:27:14.000";
        String end = "2013-01-15 14:27:14.000";
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.getSegmentConfig().setAutoMergeEnabled(true);
        prjManager.updateProject(copy);
        val loadingRange = createDataLoadingRange(DateFormat.stringToMillis(start), DateFormat.stringToMillis(end));
        val ranges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(loadingRange);
        Assert.assertEquals(4, ranges.size());
        //12/12/25-13/01/01 00:00
        Assert.assertEquals(start, DateFormat.formatToTimeStr(Long.parseLong(ranges.get(0).getStart().toString())));
        Assert.assertEquals("2013-01-01 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(0).getEnd().toString())));
        //13/01/01 00:00 - 13/01/07
        Assert.assertEquals("2013-01-01 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(1).getStart().toString())));
        Assert.assertEquals("2013-01-07 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(1).getEnd().toString())));
        //13/01/07 00:00 - 13/01/14
        Assert.assertEquals("2013-01-07 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(2).getStart().toString())));
        Assert.assertEquals("2013-01-14 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(2).getEnd().toString())));

        //13/01/14 00:00 - 13/01/15
        Assert.assertEquals("2013-01-14 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(3).getStart().toString())));
        Assert.assertEquals(end, DateFormat.formatToTimeStr(Long.parseLong(ranges.get(3).getEnd().toString())));
    }

    @Test
    public void testGetSegRangeToBuildForNewDataflow_3DaysVolatile() {
        //2013/01/01-2013/01/15
        String start = "2013-01-01 00:00:00.000";
        String end = "2013-01-15 14:27:14.000";

        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.getSegmentConfig().getVolatileRange().setVolatileRangeNumber(3L);
        copy.getSegmentConfig().getVolatileRange().setVolatileRangeEnabled(true);
        copy.getSegmentConfig().setAutoMergeEnabled(true);

        prjManager.updateProject(copy);

        val loadingRange = createDataLoadingRange(DateFormat.stringToMillis(start), DateFormat.stringToMillis(end));
        val ranges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(loadingRange);
        Assert.assertEquals(5, ranges.size());
        //13/01/01 00:00 - 13/01/07
        Assert.assertEquals(start, DateFormat.formatToTimeStr(Long.parseLong(ranges.get(0).getStart().toString())));
        Assert.assertEquals("2013-01-07 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(0).getEnd().toString())));
        //13/01/07 00:00 - 13/01/12
        Assert.assertEquals("2013-01-07 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(1).getStart().toString())));
        Assert.assertEquals("2013-01-12 14:27:14.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(1).getEnd().toString())));

        //13/01/12 00:00 - 13/01/13
        Assert.assertEquals("2013-01-12 14:27:14.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(2).getStart().toString())));
        Assert.assertEquals("2013-01-13 14:27:14.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(2).getEnd().toString())));

        //13/01/13 00:00 - 13/01/14
        Assert.assertEquals("2013-01-13 14:27:14.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(3).getStart().toString())));
        Assert.assertEquals("2013-01-14 14:27:14.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(3).getEnd().toString())));

        //13/01/14 00:00 - 13/01/15
        Assert.assertEquals("2013-01-14 14:27:14.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(4).getStart().toString())));
        Assert.assertEquals(end, DateFormat.formatToTimeStr(Long.parseLong(ranges.get(4).getEnd().toString())));
    }

    @Test
    public void testGetSegRangeToBuildForNewDataflow_YearMonthAndWeek() {
        //2010/12/24-2012/01/04
        //        long start = 1293194019000L;
        //        long end = 1325680419000L;
        String start = "2010-12-24 20:33:39.000";
        String end = "2012-01-04 20:33:39.000";
        val loadingRange = createDataLoadingRange(DateFormat.stringToMillis(start), DateFormat.stringToMillis(end));
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.getSegmentConfig().getAutoMergeTimeRanges().add(AutoMergeTimeEnum.YEAR);
        copy.getSegmentConfig().setAutoMergeEnabled(true);
        prjManager.updateProject(copy);
        val ranges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(loadingRange);
        Assert.assertEquals(4, ranges.size());
        //10/12/24 00:00 - 11/01/01
        Assert.assertEquals(start, DateFormat.formatToTimeStr(Long.parseLong(ranges.get(0).getStart().toString())));
        Assert.assertEquals("2011-01-01 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(0).getEnd().toString())));
        //11/01/01 00:00 - 12/01/01
        Assert.assertEquals("2011-01-01 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(1).getStart().toString())));
        Assert.assertEquals("2012-01-01 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(1).getEnd().toString())));

        //12/01/01 00:00 - 12/01/02
        Assert.assertEquals("2012-01-01 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(2).getStart().toString())));
        Assert.assertEquals("2012-01-02 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(2).getEnd().toString())));

        //12/01/02 00:00 - 12/01/04
        Assert.assertEquals("2012-01-02 00:00:00.000",
                DateFormat.formatToTimeStr(Long.parseLong(ranges.get(3).getStart().toString())));
        Assert.assertEquals(end, DateFormat.formatToTimeStr(Long.parseLong(ranges.get(3).getEnd().toString())));
    }

    private NDataLoadingRange createDataLoadingRange(long start, long end) {
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.LEAF_CATEG_ID";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start, end);
        dataLoadingRange.setCoveredRange(range);
        return dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
    }

    @Test
    public void testGetQueryableSegmentRange_NoModel() {
        String start = "2010-12-24 20:33:39.000";
        String end = "2012-01-04 20:33:39.000";
        val loadingRange = createDataLoadingRange(DateFormat.stringToMillis(start), DateFormat.stringToMillis(end));
        loadingRange.setTableName("DEFAULT.TEST_ACCOUNT");
        val range = dataLoadingRangeManager.getQuerableSegmentRange(loadingRange);
        Assert.assertEquals(start, DateFormat.formatToTimeStr(Long.parseLong(range.getStart().toString())));
        Assert.assertEquals(end, DateFormat.formatToTimeStr(Long.parseLong(range.getEnd().toString())));
    }
}
