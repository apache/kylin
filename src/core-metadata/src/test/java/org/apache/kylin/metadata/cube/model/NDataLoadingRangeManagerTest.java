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

import java.io.IOException;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
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
    public void testAppendSegRangeErrorCase() throws IOException {
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
    public void testCreateAndUpdateDataLoadingRange() throws IOException {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
        Assert.assertTrue(savedDataLoadingRange.getProject().equals(DEFAULT_PROJECT));
    }

    @Test
    public void testCreateDataLoadingRange_StringColumn() throws IOException {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.LSTG_FORMAT_NAME";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setPartitionDateFormat("YYYY");
        dataLoadingRange.setColumnName(columnName);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        Assert.assertTrue(savedDataLoadingRange.getProject().equals(DEFAULT_PROJECT));
        Assert.assertTrue(savedDataLoadingRange.getColumnName().equals(columnName));
    }

    @Test
    public void testCreateDataLoadingRange_IntegerColumn() throws IOException {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.LEAF_CATEG_ID";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setPartitionDateFormat("YYYY");
        dataLoadingRange.setColumnName(columnName);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
        Assert.assertTrue(savedDataLoadingRange.getProject().equals(DEFAULT_PROJECT));
        Assert.assertTrue(savedDataLoadingRange.getColumnName().equals(columnName));
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
    public void testGetQuerableSegmentRange_NoModel() {
        String start = "2010-12-24 20:33:39.000";
        String end = "2012-01-04 20:33:39.000";
        val loadingRange = createDataLoadingRange(DateFormat.stringToMillis(start), DateFormat.stringToMillis(end));
        loadingRange.setTableName("DEFAULT.TEST_ACCOUNT");
        val range = dataLoadingRangeManager.getQuerableSegmentRange(loadingRange);
        Assert.assertEquals(start, DateFormat.formatToTimeStr(Long.parseLong(range.getStart().toString())));
        Assert.assertEquals(end, DateFormat.formatToTimeStr(Long.parseLong(range.getEnd().toString())));
    }

    @Test
    public void testGetQuerableSegmentRange_HasModels() {
        String start = "2010-12-24 20:33:39.000";
        String end = "2012-01-04 20:33:39.000";
        removeAllSegments();
        val segments = new Segments<NDataSegment>();
        val segments2 = new Segments<NDataSegment>();

        val loadingRange = createDataLoadingRange(DateFormat.stringToMillis(start), DateFormat.stringToMillis(end));
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);

        end = "2011-05-18 09:00:19.000";
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis(start),
                DateFormat.stringToMillis(end));
        NDataflow df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegment);

        segmentRange = new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis(start),
                DateFormat.stringToMillis(end));
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments2.add(dataSegment);

        start = end;
        end = "2012-01-04 20:33:39.000";
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis(start),
                DateFormat.stringToMillis(end));
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegment);

        segmentRange = new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis(start),
                DateFormat.stringToMillis(end));
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.NEW);
        segments2.add(dataSegment);

        NDataflowUpdate update = new NDataflowUpdate(dataflowManager.getDataflowByModelAlias("nmodel_basic").getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        update.setStatus(RealizationStatusEnum.ONLINE);
        dataflowManager.updateDataflow(update);

        update = new NDataflowUpdate(dataflowManager.getDataflowByModelAlias("nmodel_basic_inner").getUuid());
        update.setToUpdateSegs(segments2.toArray(new NDataSegment[segments.size()]));
        update.setStatus(RealizationStatusEnum.ONLINE);
        dataflowManager.updateDataflow(update);

        dataflowManager.updateDataflowStatus("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", RealizationStatusEnum.OFFLINE);

        val range = dataLoadingRangeManager.getQuerableSegmentRange(loadingRange);
        Assert.assertEquals("2010-12-24 20:33:39.000",
                DateFormat.formatToTimeStr(Long.parseLong(range.getStart().toString())));
        Assert.assertEquals("2011-05-18 09:00:19.000",
                DateFormat.formatToTimeStr(Long.parseLong(range.getEnd().toString())));

        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        val segs = df.getQueryableSegments();
        Assert.assertEquals(1, segs.size());

        Assert.assertEquals("2010-12-24 20:33:39.000",
                DateFormat.formatToTimeStr(Long.parseLong(segs.get(0).getSegRange().getStart().toString())));
        Assert.assertEquals("2011-05-18 09:00:19.000",
                DateFormat.formatToTimeStr(Long.parseLong(segs.get(0).getSegRange().getEnd().toString())));

    }

    private void removeAllSegments() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);

        df = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        // remove the existed seg
        update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }
}
