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

package io.kyligence.kap.secondstorage;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.IndexPlanService;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.ModelSemanticHelper;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;

import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.ProjectLoadRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.val;

public class IncrementalTest implements JobWaiter {
    private static final String modelName = "test_table_index";
    static private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";

    public static String getProject() {
        return project;
    }

    static private final String project = "table_index_incremental";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "org.apache.kylin.query.SQLPushDownExtensions"));
    private final SparkSession sparkSession = sharedSpark.getSpark();
    @ClassRule
    public static ClickHouseClassRule clickHouseClassRule = new ClickHouseClassRule(1);
    public EnableTestUser enableTestUser = new EnableTestUser();
    public EnableClickHouseJob test = new EnableClickHouseJob(clickHouseClassRule.getClickhouse(), 1, project,
            Collections.singletonList(modelId), "src/test/resources/ut_meta");
    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(test);
    private SecondStorageService secondStorageService = new SecondStorageService();
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();
    private IndexDataConstructor indexDataConstructor;

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    private final ModelBuildService modelBuildService = Mockito.spy(ModelBuildService.class);

    @Mock
    private final ModelSemanticHelper modelSemanticHelper = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Before
    public void setUp() {
        System.setProperty("kylin.second-storage.wait-index-build-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);

        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "indexPlanService", indexPlanService);
        ReflectionTestUtils.setField(modelService, "semanticUpdater", modelSemanticHelper);
        ReflectionTestUtils.setField(modelService, "modelBuildService", modelBuildService);

        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageEndpoint.setModelService(modelService);
        secondStorageService.setModelService(modelService);
        indexDataConstructor = new IndexDataConstructor(project);

        secondStorageService.setAclEvaluate(aclEvaluate);
    }

    private void buildIncrementalLoadQuery(String start, String end) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = modelId;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        val timeRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val indexes = new HashSet<>(df.getIndexPlan().getAllLayouts());
        indexDataConstructor.buildIndex(dfName, timeRange, indexes, true);
    }

    private void mergeSegments(List<String> segIds) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(modelId);
        val jobManager = JobManager.getInstance(config, getProject());
        long start = Long.MAX_VALUE;
        long end = -1;
        for (String id : segIds) {
            val segment = df.getSegment(id);
            val segmentStart = segment.getTSRange().getStart();
            val segmentEnd = segment.getTSRange().getEnd();
            if (segmentStart < start)
                start = segmentStart;
            if (segmentEnd > end)
                end = segmentEnd;
        }

        val mergeSeg = dfMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(start, end), true);
        val jobParam = new JobParam(mergeSeg, modelId, enableTestUser.getUser());
        val jobId = jobManager.mergeSegmentJob(jobParam);
        waitJobFinish(getProject(), jobId);
    }

    @Test
    public void testMergeSegmentWhenSegmentNotInSecondStorage() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        // clean first segment
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        secondStorageEndpoint.cleanStorage(request, segs.subList(0, 1));

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val jobs = executableManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        jobs.forEach(job -> waitJobFinish(getProject(), job.getId()));

        mergeSegments(segs);
        Assert.assertEquals(1, dataflowManager.getDataflow(modelId).getQueryableSegments().size());
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        val expectSegments = dataflowManager.getDataflow(modelId).getSegments().stream().map(NDataSegment::getId)
                .collect(Collectors.toSet());
        Assert.assertTrue(tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0)
                .containSegments(expectSegments));
    }

    @Test
    public void testSegmentNoDataAndProjectReload() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        // clean first segment
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        dataflowManager.updateDataflowDetailsLayouts(dataflowManager.getDataflow(modelId).getFirstSegment(),
                Collections.emptyList());

        val request = new ProjectLoadRequest();
        request.setProjects(ImmutableList.of(project));

        EnvelopeResponse<List<ProjectRecoveryResponse>> response = secondStorageEndpoint.projectLoad(request);
        Assert.assertEquals("000", response.getCode());
        Assert.assertNotNull(response.getData());
        Assert.assertEquals(1, response.getData().size());
        Assert.assertNotNull(response.getData().get(0).getSubmittedModels());
        Assert.assertEquals(1, response.getData().get(0).getSubmittedModels().size());

        response.getData().forEach(res -> res.getJobs().forEach(job -> waitJobEnd(getProject(), job.getJobId())));

        dataflowManager.getDataflow(modelId).getSegments()
                .forEach(segment -> dataflowManager.updateDataflowDetailsLayouts(segment, Collections.emptyList()));
        response = secondStorageEndpoint.projectLoad(request);
        Assert.assertEquals("000", response.getCode());
        Assert.assertNotNull(response.getData());
        Assert.assertEquals(1, response.getData().size());
        Assert.assertNotNull(response.getData().get(0).getFailedModels());
        Assert.assertEquals(1, response.getData().get(0).getFailedModels().size());
    }
}
