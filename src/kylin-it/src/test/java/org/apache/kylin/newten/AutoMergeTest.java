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
package org.apache.kylin.newten;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.job.NSparkMergingJob;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.SegmentAutoMergeUtil;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.NDataLoadingRange;
import org.apache.kylin.metadata.cube.model.NDataLoadingRangeManager;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.VolatileRange;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import lombok.val;

@RunWith(TimeZoneTestRunner.class)
public class AutoMergeTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @BeforeClass
    public static void beforeAll() {
        // load NSparkMergingJob class
        new NSparkMergingJob();
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    private void removeAllSegments() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }

    private void mockAddSegmentSuccess()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject(DEFAULT_PROJECT);
        val copy = prjManager.copyForWrite(prj);
        copy.getSegmentConfig().setAutoMergeEnabled(true);
        prjManager.updateProject(copy);
        SegmentAutoMergeUtil.autoMergeSegments(DEFAULT_PROJECT, df.getUuid(), "ADMIN");
    }

    private void createDataloadingRange() throws IOException {
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        dataLoadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        NDataLoadingRangeManager.getInstance(getTestConfig(), DEFAULT_PROJECT).createDataLoadingRange(dataLoadingRange);
    }

    @Test
    public void testRetention_2Week() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //two days,not enough for a week ,not merge
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = addDay("2010-01-01", i);
            end = addDay("2010-01-02", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(0, executables.size());
    }

    @Test
    public void testAutoMergeSegmentsByWeek_FridayAndSaturday_NotMerge() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //two days,not enough for a week ,not merge
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = addDay("2010-01-01", i);
            end = addDay("2010-01-02", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        //clear all events
        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(0, executables.size());
    }

    @Test
    public void testAutoMergeSegmentsByWeek_WithoutVolatileRange_Merge() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 4 days ,2010/01/01 8:00 - 2010/01/04 8:00， friday to monday, merge
        for (int i = 0; i <= 3; i++) {
            //01-01 friday
            start = addDay("2010-01-01", i);
            end = addDay("2010-01-02", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        //clear all events
        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());

        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                //merge 2010/01/01 00:00 - 2010/01/04 00:00
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-01 00:00:00"),
                        dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegment(segId)
                                .getSegRange().getStart());
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-04 00:00:00"), dataflowManager
                        .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegment(segId).getSegRange().getEnd());
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByWeek_WithThreeDaysVolatileRange_MergeFirstWeek() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 9 days ,2010/01/01 - 2010/01/10， volatileRange 3 days
        for (int i = 0; i <= 9; i++) {
            //01-01 friday
            start = addDay("2010-01-01", i);
            end = addDay("2010-01-02", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        //set 3days volatile ,and just merge the first week
        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        VolatileRange volatileRange = new VolatileRange();
        volatileRange.setVolatileRangeNumber(3);
        volatileRange.setVolatileRangeEnabled(true);
        volatileRange.setVolatileRangeType(AutoMergeTimeEnum.DAY);
        modelUpdate.getSegmentConfig().setVolatileRange(volatileRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-01 00:00:00"),
                        dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegment(segId)
                                .getSegRange().getStart());
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-04 00:00:00"), dataflowManager
                        .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegment(segId).getSegRange().getEnd());
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByWeek_WithOneDaysVolatileRange_CornerCase() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 8 days ,2010/01/04 - 2010/01/11， volatileRange 1 days
        for (int i = 0; i <= 7; i++) {
            //01-01 friday
            start = addDay("2010-01-04", i);
            end = addDay("2010-01-05", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        //set 1 days volatile ,and just merge the first week
        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        VolatileRange volatileRange = new VolatileRange();
        volatileRange.setVolatileRangeNumber(1);
        volatileRange.setVolatileRangeEnabled(true);
        volatileRange.setVolatileRangeType(AutoMergeTimeEnum.DAY);
        modelUpdate.getSegmentConfig().setVolatileRange(volatileRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        //merge 1/4/-1/10
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-04 00:00:00"), dataflowManager
                        .getDataflowByModelAlias("nmodel_basic").getSegment(segId).getSegRange().getStart());
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-11 00:00:00"), dataflowManager
                        .getDataflowByModelAlias("nmodel_basic").getSegment(segId).getSegRange().getEnd());
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByWeek_SegmentsHasOneDayGap_MergeSecondWeek() throws Exception {

        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 2 week,and the first week has gap segment
        //test 9 days ,2010/01/01 00:00 - 2010/01/10 00:00，remove 2010/01/02,merge the second week
        for (int i = 0; i <= 9; i++) {
            //01-01 friday
            start = addDay("2010-01-01", i);
            end = addDay("2010-01-02", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        update = new NDataflowUpdate(df.getUuid());
        //remove 2010-01-02
        update.setToRemoveSegs(new NDataSegment[] { df.getSegments().get(1) });
        dataflowManager.updateDataflow(update);

        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                start = Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegment(segId).getSegRange().getStart().toString());
                end = Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegment(segId).getSegRange().getEnd().toString());
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-04 00:00:00"), start);
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-11 00:00:00"), end);
            }
        }

    }

    @Test
    public void testAutoMergeSegmentsByWeek_WhenSegmentsContainBuildingSegment() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 2 week,and the first week has building segment
        //test 9 days ,2010/01/01 00:00 - 2010/01/10 00:00， 2010/01/02 building,merge the second week
        for (int i = 0; i <= 9; i++) {
            //01-01 friday
            start = addDay("2010-01-01", i);
            end = addDay("2010-01-02", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            if (i != 1) {
                dataSegment.setStatus(SegmentStatusEnum.READY);
            }
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                start = Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegment(segId).getSegRange().getStart().toString());
                end = Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegment(segId).getSegRange().getEnd().toString());
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-04 00:00:00"), start);
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-11 00:00:00"), end);
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByWeek_HasBigSegment_Merge() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 2 days and a big segment,merge the first week
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = addDay("2010-01-01", i);
            end = addDay("2010-01-02", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        //a big segment
        start = DateFormat.stringToMillis("2010-01-03 00:00:00");
        end = DateFormat.stringToMillis("2010-01-11 00:00:00");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegment);

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                start = Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegment(segId).getSegRange().getStart().toString());
                end = Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegment(segId).getSegRange().getEnd().toString());
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-01 00:00:00"), start);
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-03 00:00:00"), end);
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByWeek_FirstDayOfWeekWSunday_Merge() throws Exception {
        getTestConfig().setProperty("kylin.metadata.first-day-of-week", "sunday");
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 2 days and a big segment,merge the first week
        for (int i = 0; i <= 9; i++) {
            //01-01 friday
            start = addDay("2010-01-03", i);
            end = addDay("2010-01-04", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                start = Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegment(segId).getSegRange().getStart().toString());
                end = Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegment(segId).getSegRange().getEnd().toString());
                //2010/1/3 sunday - 2010/1/10 sunday
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-03 00:00:00"), start);
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-10 00:00:00"), end);
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByHour_PASS() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //13min per segment
        for (int i = 0; i <= 5; i++) {
            //01-01 00:00 - 01:05 merge one hour
            start = SegmentRange.dateToLong("2010-01-01") + i * 1000 * 60 * 13;
            end = SegmentRange.dateToLong("2010-01-01") + (i + 1) * 1000 * 60 * 13;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> ranges = new ArrayList<>();
        ranges.add(AutoMergeTimeEnum.HOUR);
        modelUpdate.getSegmentConfig().setAutoMergeTimeRanges(ranges);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-01 00:00:00"),
                        Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment(segId).getSegRange().getStart().toString()));
                Assert.assertEquals(DateFormat.stringToMillis("2010-01-01 00:52:00"),
                        Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment(segId).getSegRange().getEnd().toString()));
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByMonth() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> ranges = new ArrayList<>();
        ranges.add(AutoMergeTimeEnum.MONTH);
        modelUpdate.getSegmentConfig().setAutoMergeTimeRanges(ranges);
        dataModelManager.updateDataModelDesc(modelUpdate);

        //4week segment ,and four one day segment ,2010/12/01 - 2011/1/02
        for (int i = 0; i <= 3; i++) {
            start = addDay("2010-12-01", 7 * i);
            end = addDay("2010-12-08", 7 * i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        for (int i = 0; i <= 3; i++) {
            start = addDay("2010-12-29", i);
            end = addDay("2010-12-30", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        //clear all events
        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                Assert.assertEquals(DateFormat.stringToMillis("2010-12-01 00:00:00"),
                        Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment(segId).getSegRange().getStart().toString()));
                Assert.assertEquals(DateFormat.stringToMillis("2011-01-01 00:00:00"),
                        Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment(segId).getSegRange().getEnd().toString()));
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByYear() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        // 2010/10月 -2011/2月 merge 2010/10-2010/12
        // use calendar to void Daylight Saving Time problem
        for (int i = 0; i <= 4; i++) {
            start = addDay("2010-10-01", 30 * i);
            end = addDay("2010-10-31", 30 * i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> timeRanges = new ArrayList<>();
        timeRanges.add(AutoMergeTimeEnum.YEAR);
        modelUpdate.getSegmentConfig().setAutoMergeTimeRanges(timeRanges);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        deleteAllJobs(DEFAULT_PROJECT);
        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                Assert.assertEquals(DateFormat.stringToMillis("2010-10-01 00:00:00"),
                        Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment(segId).getSegRange().getStart().toString()));
                Assert.assertEquals(DateFormat.stringToMillis("2010-12-30 00:00:00"),
                        Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment(segId).getSegRange().getEnd().toString()));
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByDay() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //2010/10/01 8:00 - 2010/10/02 06:15
        for (int i = 0; i <= 9; i++) {
            start = SegmentRange.dateToLong("2010-10-01 08:00:00") + i * 8100000L;
            end = start + 8100000L;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> timeRanges = new ArrayList<>();
        timeRanges.add(AutoMergeTimeEnum.DAY);
        modelUpdate.getSegmentConfig().setAutoMergeTimeRanges(timeRanges);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        deleteAllJobs(DEFAULT_PROJECT);

        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(1, executables.size());
        for (val executable : executables) {
            if (executable instanceof NSparkMergingJob) {
                val segId = executable.getTargetSegments().get(0);
                Assert.assertEquals(DateFormat.stringToMillis("2010-10-01 08:00:00"),
                        Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment(segId).getSegRange().getStart().toString()));
                Assert.assertEquals(DateFormat.stringToMillis("2010-10-01 23:45:00"),
                        Long.parseLong(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment(segId).getSegRange().getEnd().toString()));
            }
        }
    }

    @Test
    public void testAutoMergeSegmentsByWeek_BigGapOverlapTwoSection_NotMerge() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //2010/10/04 2010/10/05
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = addDay("2010-01-04", i);
            end = addDay("2010-01-05", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        //2010/10/11 2010/10/12
        for (int i = 2; i <= 3; i++) {
            //01-01 friday
            start = addDay("2010-01-09", i);
            end = addDay("2010-01-10", i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> timeRanges = new ArrayList<>();
        timeRanges.add(AutoMergeTimeEnum.WEEK);
        modelUpdate.getSegmentConfig().setAutoMergeTimeRanges(timeRanges);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        deleteAllJobs(DEFAULT_PROJECT);

        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(0, executables.size());

    }

    @Test
    public void testAutoMergeSegmentsByWeek_FirstWeekNoSegment_NotMerge() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //2010/10/04 2010/10/05
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = addDay("2010-01-03", 2 * i);
            end = addDay("2010-01-05", 2 * i);
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        deleteAllJobs(DEFAULT_PROJECT);

        mockAddSegmentSuccess();
        val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
        Assert.assertEquals(0, executables.size());

    }

    @Ignore("TODO: remove or adapt")
    @Test
    public void testAutoMergeSegments_2YearsRange_OneHourPerSegment() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Class<RootPersistentEntity> clazz = RootPersistentEntity.class;
        Field field = clazz.getDeclaredField("isCachedAndShared");
        Unsafe.changeAccessibleObject(field, true);
        field.set(df, false);
        NDataModel model = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> autoMergeTimeEnumList = new ArrayList<>();
        autoMergeTimeEnumList.add(AutoMergeTimeEnum.YEAR);
        autoMergeTimeEnumList.add(AutoMergeTimeEnum.MONTH);
        autoMergeTimeEnumList.add(AutoMergeTimeEnum.WEEK);
        autoMergeTimeEnumList.add(AutoMergeTimeEnum.DAY);
        modelUpdate.getSegmentConfig().setAutoMergeTimeRanges(autoMergeTimeEnumList);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        int eventsMergeYear = 0;
        int eventsMergeMonth = 0;
        int eventsMergeWeek = 0;
        int eventsMergeDay = 0;
        int allEvent = 0;
        Segments<NDataSegment> segments = df.getSegments();
        //2010/01/10 00:00:00 -2012-02-10 01:00:00 one hour per segment
        long start = 1263081600000L;
        long end = 1263081600000L + 3600000L;
        int i = 0;
        long mergeStart;
        long mergeEnd;
        while (end <= 1328835600000L) {
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            NDataSegment newSegment = NDataSegment.empty();
            newSegment.setSegmentRange(segmentRange);
            newSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(newSegment);
            df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            field.set(df, false);
            df.setSegments(segments);
            deleteAllJobs(DEFAULT_PROJECT);
            mockAddSegmentSuccess();
            val executables = getRunningExecutables(DEFAULT_PROJECT, df.getModel().getId());
            if (executables.size() > 0) {
                allEvent++;
                for (val executable : executables) {
                    if (executable instanceof NSparkMergingJob) {
                        val segId = executable.getTargetSegments().get(0);
                        mergeStart = Long.parseLong(df.getSegment(segId).getSegRange().getStart().toString());
                        mergeEnd = Long.parseLong(df.getSegment(segId).getSegRange().getEnd().toString());

                        if (mergeEnd - mergeStart > 2678400000L) {
                            eventsMergeYear++;
                        } else if (mergeEnd - mergeStart > 604800000L) {
                            eventsMergeMonth++;
                        } else if (mergeEnd - mergeStart > 86400000L
                                || segments.getMergeEnd(mergeStart, AutoMergeTimeEnum.WEEK) == mergeEnd) {
                            eventsMergeWeek++;
                        } else {
                            eventsMergeDay++;
                        }

                        mockMergeSegments(i, dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .getSegment((executables.get(0)).getTargetSegments().get(0)).getSegRange());
                    }
                }
                i += 2;
            } else {
                i++;
            }
            start += 3600000L;
            end += 3600000L;
        }
        Assert.assertEquals(2, eventsMergeYear);
        Assert.assertEquals(23, eventsMergeMonth);
        Assert.assertEquals(105, eventsMergeWeek);
        Assert.assertEquals(631, eventsMergeDay);
        Assert.assertEquals(761, allEvent);

        //check final segments
        Segments<NDataSegment> finalSegments = df.getSegments();
        Assert.assertEquals(9, finalSegments.size());
        //2010/01/10 - 2011/01/01 00:00:00
        Assert.assertEquals(1263081600000L, Long.parseLong(finalSegments.get(0).getSegRange().getStart().toString()));
        Assert.assertEquals(1293840000000L, Long.parseLong(finalSegments.get(0).getSegRange().getEnd().toString()));
        //2011/01/01 - 2012/01/01 00:00:00
        Assert.assertEquals(1293840000000L, Long.parseLong(finalSegments.get(1).getSegRange().getStart().toString()));
        Assert.assertEquals(1325376000000L, Long.parseLong(finalSegments.get(1).getSegRange().getEnd().toString()));
        //2012/01/01 - 2012/02/01 00:00:00
        Assert.assertEquals(1325376000000L, Long.parseLong(finalSegments.get(2).getSegRange().getStart().toString()));
        Assert.assertEquals(1328054400000L, Long.parseLong(finalSegments.get(2).getSegRange().getEnd().toString()));
        //2012/02/01 - 2012/02/06 00:00:00 week
        Assert.assertEquals(1328054400000L, Long.parseLong(finalSegments.get(3).getSegRange().getStart().toString()));
        Assert.assertEquals(1328486400000L, Long.parseLong(finalSegments.get(3).getSegRange().getEnd().toString()));
        //2012/02/06 - 2012/02/07 00:00:00 day
        Assert.assertEquals(1328486400000L, Long.parseLong(finalSegments.get(4).getSegRange().getStart().toString()));
        Assert.assertEquals(1328572800000L, Long.parseLong(finalSegments.get(4).getSegRange().getEnd().toString()));
        //2012/02/07 - 2012/02/08 00:00:00 day
        Assert.assertEquals(1328572800000L, Long.parseLong(finalSegments.get(5).getSegRange().getStart().toString()));
        Assert.assertEquals(1328659200000L, Long.parseLong(finalSegments.get(5).getSegRange().getEnd().toString()));
        //2012/02/08 - 2012/02/09 00:00:00 day
        Assert.assertEquals(1328659200000L, Long.parseLong(finalSegments.get(6).getSegRange().getStart().toString()));
        Assert.assertEquals(1328745600000L, Long.parseLong(finalSegments.get(6).getSegRange().getEnd().toString()));
        //2012/02/09 - 2012/02/10 00:00:00 day
        Assert.assertEquals(1328745600000L, Long.parseLong(finalSegments.get(7).getSegRange().getStart().toString()));
        Assert.assertEquals(1328832000000L, Long.parseLong(finalSegments.get(7).getSegRange().getEnd().toString()));
        //2012/02/10 00:00 - 2012/02/10 01:00:00 hour
        Assert.assertEquals(1328832000000L, Long.parseLong(finalSegments.get(8).getSegRange().getStart().toString()));
        Assert.assertEquals(1328835600000L, Long.parseLong(finalSegments.get(8).getSegRange().getEnd().toString()));
    }

    private void mockMergeSegments(int i, SegmentRange segmentRange) {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Segments<NDataSegment> segments = new Segments<>();
        for (NDataSegment segment : dataflow.getSegments()) {
            if (segmentRange.contains(segment.getSegRange())) {
                segments.add(segment);
            }
        }
        NDataSegment mergedSegment = NDataSegment.empty();
        mergedSegment.setStatus(SegmentStatusEnum.READY);
        mergedSegment.setId(i + "");
        mergedSegment.setSegmentRange(segmentRange);
        dataflow.getSegments().removeAll(segments);
        dataflow.getSegments().add(mergedSegment);

    }

    public long addDay(String base, int inc) {
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        calendar.setTimeInMillis(SegmentRange.dateToLong(base));
        calendar.add(Calendar.DAY_OF_MONTH, inc);
        return calendar.getTimeInMillis();
    }

    private List<AbstractExecutable> getRunningExecutables(String project, String model) {
        return NExecutableManager.getInstance(getTestConfig(), project).getRunningExecutables(project, model);
    }

    private void deleteAllJobs(String project) {
        NExecutableManager.getInstance(getTestConfig(), project).deleteAllJob();
    }
}
