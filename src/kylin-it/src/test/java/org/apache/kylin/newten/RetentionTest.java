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

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.RetentionRange;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import lombok.val;

@RunWith(TimeZoneTestRunner.class)
public class RetentionTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

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
        NDataflow df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }

    private void mockAddSegmentSuccess() {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        dataflowManager.handleRetention(df);
    }

    @Test
    public void testRetention_2Week() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //two days,not enough for a week
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000L;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000L;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(2);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.WEEK);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        //no retention
        Assert.assertEquals(2, df.getSegments().size());
    }

    @Test
    public void testRetention_2Week_3WeekDataCornerCase() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //3 week data last period is full week
        for (int i = 0; i <= 2; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000L * 7;
            end = SegmentRange.dateToLong("2010-01-11") + i * 86400000L * 7;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(2);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.WEEK);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(2, df.getSegments().size());
        //01-11
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-11 00:00:00"),
                df.getSegments().get(0).getSegRange().getStart());
        //01-18
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-25 00:00:00"),
                df.getSegments().get(1).getSegRange().getEnd());
    }

    @Test
    public void testRetention_2Week_3WeekAndOneDayData() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //3 week data last period is full week
        for (int i = 0; i <= 2; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000L * 7;
            end = SegmentRange.dateToLong("2010-01-11") + i * 86400000L * 7;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }

        //one more day
        start = SegmentRange.dateToLong("2010-01-25");
        end = SegmentRange.dateToLong("2010-01-26");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        dataflowManager.appendSegment(df, segmentRange);

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(2);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.WEEK);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");

        Assert.assertEquals(3, df.getSegments().size());
        //01/11
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-11 00:00:00"),
                df.getSegments().get(0).getSegRange().getStart());
        //01/26
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-26 00:00:00"),
                df.getSegments().getLastSegment().getSegRange().getEnd());
    }

    @Test
    public void testRetention_1Month_9WeekData() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //5 week data last period is not full month
        for (int i = 0; i <= 8; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000L * 7;
            end = SegmentRange.dateToLong("2010-01-11") + i * 86400000L * 7;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(1);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.MONTH);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        //retention
        Assert.assertEquals(4, df.getSegments().size());
        //02/08
        Assert.assertEquals(DateFormat.stringToMillis("2010-02-08 00:00:00"),
                df.getSegments().get(0).getSegRange().getStart());
        //03/08
        Assert.assertEquals(DateFormat.stringToMillis("2010-03-08 00:00:00"),
                df.getSegments().getLastSegment().getSegRange().getEnd());

    }

    @Test
    public void testRetention_1Month_5WeekData() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //5 week data last period is not full month
        for (int i = 0; i <= 4; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000L * 7;
            end = SegmentRange.dateToLong("2010-01-11") + i * 86400000L * 7;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(1);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.MONTH);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(5, df.getSegments().size());
        //01/04
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-04 00:00:00"),
                df.getSegments().get(0).getSegRange().getStart());
        //02/08
        Assert.assertEquals(DateFormat.stringToMillis("2010-02-08 00:00:00"),
                df.getSegments().getLastSegment().getSegRange().getEnd());
    }

}
