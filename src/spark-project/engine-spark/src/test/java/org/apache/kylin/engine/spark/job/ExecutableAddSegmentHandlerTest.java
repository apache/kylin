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

import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLoadingRange;
import org.apache.kylin.metadata.cube.model.NDataLoadingRangeManager;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@MetadataInfo
class ExecutableAddSegmentHandlerTest {
    private NExecutableManager manager;
    private NDataflowManager dfManager;
    private NDataLoadingRangeManager dataLoadingRangeManager;

    private static final String DEFAULT_PROJECT = "default";

    @BeforeEach
    void setup() {
        val config = KylinConfig.getInstanceFromEnv();
        manager = NExecutableManager.getInstance(config, DEFAULT_PROJECT);
        dfManager = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(config, DEFAULT_PROJECT);
        for (String jobPath : manager.getJobs()) {
            manager.deleteJob(jobPath);
        }
    }

    @Test
    void handleFinishedCondition() {
        val model = RandomUtil.randomUUIDStr();
        val job = new DefaultExecutableOnModel();
        job.setProject(DEFAULT_PROJECT);
        job.setTargetSubject(model);
        manager.addJob(job);

        try {
            val handler = new ExecutableAddSegmentHandler(DEFAULT_PROJECT, model, "test", null, job.getId());
            handler.handleFinished();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    void handleDiscardOrSuicidalNoLayoutIds() {
        val job = new DefaultExecutableOnModel();
        val df = dfManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject("default");
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        manager.addJob(job);
        val handler = new ExecutableAddSegmentHandler(DEFAULT_PROJECT, df.getModel().getUuid(), "test",
                df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()).get(0), job.getId());
        handler.handleDiscardOrSuicidal();
    }

    @Test
    void handleDiscardOrSuicidal() {
        val job = new DefaultExecutableOnModel();
        val df = dfManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject("default");
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        manager.addJob(job);
        val handler = new ExecutableAddSegmentHandler(DEFAULT_PROJECT, df.getModel().getUuid(), "test",
                df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()).get(0), job.getId());
        handler.handleDiscardOrSuicidal();
    }

    @Test
    void handleDiscardOrSuicidalLagBehindDataflow() {
        val job = new DefaultExecutableOnModel();
        val df = dfManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dfManager.updateDataflow(df.getId(), copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND));

        String start = "2010-12-24 20:33:39.000";
        String end = "2012-01-04 20:33:39.000";
        createDataLoadingRange(DateFormat.stringToMillis(start), DateFormat.stringToMillis(end));

        job.setProject("default");
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        manager.addJob(job);
        val handler = new ExecutableAddSegmentHandler(DEFAULT_PROJECT, df.getModel().getUuid(), "test",
                df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()).get(0), job.getId());
        try (MockedStatic<SegmentUtil> segmentUtil = Mockito.mockStatic(SegmentUtil.class)) {
            segmentUtil.when(() -> SegmentUtil.getSegmentsExcludeRefreshingAndMerging(Mockito.any()))
                    .thenReturn(Segments.empty());
            handler.handleDiscardOrSuicidal();
        }
    }

    @Test
    void handleDiscardOrSuicidalNotProgressing() {
        val job = new DefaultExecutableOnModel();
        val df = dfManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dfManager.updateDataflow(df.getId(), copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND));

        String start = "2010-12-24 20:33:39.000";
        String end = "2012-01-04 20:33:39.000";
        createDataLoadingRange(DateFormat.stringToMillis(start), DateFormat.stringToMillis(end));

        job.setProject("default");
        job.setJobType(JobTypeEnum.INC_BUILD);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        manager.addJob(job);
        manager.updateJobOutput(job.getId(), ExecutableState.PAUSED);

        val handler = new ExecutableAddSegmentHandler(DEFAULT_PROJECT, df.getModel().getUuid(), "test",
                df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()).get(0), job.getId());
        try (MockedStatic<SegmentUtil> segmentUtil = Mockito.mockStatic(SegmentUtil.class)) {
            segmentUtil.when(() -> SegmentUtil.getSegmentsExcludeRefreshingAndMerging(Mockito.any()))
                    .thenReturn(Segments.empty());
            handler.handleDiscardOrSuicidal();
        }
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
}
