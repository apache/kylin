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

package org.apache.kylin.event;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.rest.util.SCD2SimplificationConvertUtil;
import org.apache.kylin.server.AbstractMVCIntegrationTestCase;
import org.apache.kylin.util.JobFinishHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSemanticTest extends AbstractMVCIntegrationTestCase {

    public static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    protected ExecutableManager executableManager;
    NDataflowManager dataflowManager;

    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @BeforeClass
    public static void beforeClass() {
        SparkJobFactoryUtils.initJobFactory();
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @AfterClass
    public static void afterClass() {
        ss.close();
    }

    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        overwriteSystemProp("kylin.job.event.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.spark.build-class-name",
                "org.apache.kylin.engine.spark.job.MockedDFBuildJob");
        JobContextUtil.getJobContext(getTestConfig());

        UnitOfWork.doInTransactionWithRetry(() -> {
            val dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
            var df = dfManager.getDataflow(MODEL_ID);

            val update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            dfManager.updateDataflow(update);

            dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                    SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("2012-03-01")));
            df = dfManager.getDataflow(MODEL_ID);
            dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                    SegmentRange.dateToLong("2012-03-01"), SegmentRange.dateToLong("2012-05-01")));

            val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
            modelManager.updateDataModel(MODEL_ID, copyForWrite -> {
                copyForWrite.setAllMeasures(copyForWrite.getAllMeasures().stream().filter(m -> m.getId() != 1011)
                        .collect(Collectors.toList()));
                copyForWrite.setManagementType(ManagementType.MODEL_BASED);
            });
            return true;
        }, getProject());

        ExecutableManager originExecutableManager = ExecutableManager.getInstance(getTestConfig(), getProject());
        executableManager = Mockito.spy(originExecutableManager);
        dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
    }

    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    public String getProject() {
        return "default";
    }

    @Test
    public void testSemanticChangedHappy() throws Exception {
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 500 * 1000));
        changeModelRequest();

        List<String> jobs = executableManager.getJobs();
        Assert.assertEquals(1, jobs.size());
        waitForJobFinish(jobs.get(0), 500 * 1000);

        NDataflow df = dfManager.getDataflow(MODEL_ID);
        Assert.assertEquals(2, df.getSegments().size());
        Assert.assertEquals(df.getIndexPlan().getAllLayouts().size(),
                df.getSegments().getLatestReadySegment().getLayoutsMap().size());
    }

    @Test
    // see issue #8740
    public void testChange_WithReadySegment() throws Exception {
        changeModelRequest();
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 600 * 1000));

        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), getProject()).updateIndexPlan(MODEL_ID, copyForWrite -> {
                List<IndexEntity> indexes = copyForWrite.getIndexes() //
                        .stream().filter(x -> x.getId() != 1000000) //
                        .collect(Collectors.toList());
                copyForWrite.setIndexes(indexes);
            });
            return true;
        }, getProject());

        // update measure
        updateMeasureRequest();
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 600 * 1000));
        Segments<NDataSegment> segments = dataflowManager.getDataflow(MODEL_ID).getSegments();
        long storageSize = 0;
        for (NDataSegment seg : segments) {
            Assert.assertEquals(SegmentStatusEnum.READY, seg.getStatus());
            storageSize += seg.getLayout(30001).getByteSize();
        }
        Assert.assertEquals(246, storageSize);
    }

    @Test
    // see issue #8820
    public void testChange_ModelWithAggGroup() throws Exception {
        changeModelRequest();
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 600 * 1000));

        // init agg group
        val group1 = JsonUtil.readValue("{\n" //
                + "        \"includes\": [1,2,3,4],\n" //
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [],\n" //
                + "          \"mandatory_dims\": [3],\n" //
                + "          \"joint_dims\": [\n" //
                + "            [1,2]\n" //
                + "          ]\n" //
                + "        }\n" //
                + "}", NAggregationGroup.class);
        val request = UpdateRuleBasedCuboidRequest.builder().project(getProject()).modelId(MODEL_ID)
                .aggregationGroups(Lists.newArrayList(group1)).build();
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/rule").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 600 * 1000));

        // update measures, throws an exception
        updateMeasureWithAgg();
    }

    private void changeModelRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val model = modelManager.getDataModelDesc(MODEL_ID);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(MODEL_ID);
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs());
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                .filter(c -> c.getStatus() == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList()));
        request.setJoinTables(
                request.getJoinTables().stream().peek(j -> j.getJoin().setType("inner")).collect(Collectors.toList()));
        request.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(request.getJoinTables()));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    private void updateMeasureRequest() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(getModelRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    private void updateMeasureWithAgg() throws Exception {
        val errorMessage = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(getModelRequest()))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn().getResponse()
                .getContentAsString();

        Assert.assertTrue(
                errorMessage.contains("The measure SUM_DEAL_AMOUNT is referenced by indexes or aggregate groups. "
                        + "Please go to the Data Asset - Model - Index page to view, delete referenced aggregate groups and indexes."));
    }

    private ModelRequest getModelRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val model = modelManager.getDataModelDesc(MODEL_ID);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(MODEL_ID);
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).peek(sm -> {
                    if (sm.getId() == 100016) {
                        sm.setExpression("MAX");
                        sm.setName("MAX_DEAL_AMOUNT");
                    }
                }).collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs());
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                .filter(c -> c.getStatus() == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList()));
        request.setJoinTables(request.getJoinTables());
        request.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(request.getJoinTables()));

        return request;
    }

    private void waitForJobFinish(String jobId, long maxWaitMilliseconds) {
        JobFinishHelper.waitJobFinish(getTestConfig(), getProject(), jobId, maxWaitMilliseconds);
    }
}
