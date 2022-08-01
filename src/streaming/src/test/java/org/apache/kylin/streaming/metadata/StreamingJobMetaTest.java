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
package org.apache.kylin.streaming.metadata;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class StreamingJobMetaTest extends NLocalFileMetadataTestCase {

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCreateBuildJob() {
        val config = KylinConfig.getInstanceFromEnv();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, PROJECT);
        val model = dataModelManager.getDataModelDesc(MODEL_ID);
        val jobStatus = JobStatusEnum.NEW;
        val jobType = JobTypeEnum.STREAMING_BUILD;
        val jobMeta = StreamingJobMeta.create(model, jobStatus, JobTypeEnum.STREAMING_BUILD);
        Assert.assertNotNull(jobMeta);
        val params = jobMeta.getParams();
        assertJobMeta(model, jobMeta, jobStatus, jobType);
        assertCommonParams(params);
        Assert.assertEquals(StreamingConstants.STREAMING_DURATION_DEFAULT,
                params.get(StreamingConstants.STREAMING_DURATION));
        Assert.assertEquals(StreamingConstants.STREAMING_MAX_OFFSETS_PER_TRIGGER_DEFAULT,
                params.get(StreamingConstants.STREAMING_MAX_OFFSETS_PER_TRIGGER));
    }

    @Test
    public void testCreateMergeJob() {
        val config = KylinConfig.getInstanceFromEnv();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, PROJECT);
        val model = dataModelManager.getDataModelDesc(MODEL_ID);
        val jobStatus = JobStatusEnum.NEW;
        val jobType = JobTypeEnum.STREAMING_MERGE;
        val jobMeta = StreamingJobMeta.create(model, jobStatus, jobType);
        Assert.assertNotNull(jobMeta);
        val params = jobMeta.getParams();
        assertJobMeta(model, jobMeta, jobStatus, jobType);
        assertCommonParams(params);
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT,
                params.get(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE));
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT,
                params.get(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD));
    }

    private void assertJobMeta(NDataModel dataModel, StreamingJobMeta jobMeta, JobStatusEnum status,
            JobTypeEnum jobType) {
        Assert.assertNotNull(jobMeta.getCreateTime());
        Assert.assertNotNull(jobMeta.getLastUpdateTime());
        Assert.assertEquals(status, jobMeta.getCurrentStatus());
        Assert.assertEquals(dataModel.getUuid(), jobMeta.getModelId());
        Assert.assertEquals(dataModel.getAlias(), jobMeta.getModelName());
        Assert.assertEquals(dataModel.getRootFactTableName(), jobMeta.getFactTableName());
        Assert.assertEquals(dataModel.getRootFactTable().getTableDesc().getKafkaConfig().getSubscribe(),
                jobMeta.getTopicName());
        Assert.assertEquals(dataModel.getOwner(), jobMeta.getOwner());
        Assert.assertEquals(StreamingUtils.getJobId(dataModel.getUuid(), jobType.name()), jobMeta.getUuid());

    }

    private void assertCommonParams(Map<String, String> params) {
        Assert.assertTrue(!params.isEmpty());
        Assert.assertEquals(StreamingConstants.SPARK_MASTER_DEFAULT, params.get(StreamingConstants.SPARK_MASTER));
        Assert.assertEquals(StreamingConstants.SPARK_DRIVER_MEM_DEFAULT,
                params.get(StreamingConstants.SPARK_DRIVER_MEM));
        Assert.assertEquals(StreamingConstants.SPARK_EXECUTOR_INSTANCES_DEFAULT,
                params.get(StreamingConstants.SPARK_EXECUTOR_INSTANCES));
        Assert.assertEquals(StreamingConstants.SPARK_EXECUTOR_CORES_DEFAULT,
                params.get(StreamingConstants.SPARK_EXECUTOR_CORES));
        Assert.assertEquals(StreamingConstants.SPARK_EXECUTOR_MEM_DEFAULT,
                params.get(StreamingConstants.SPARK_EXECUTOR_MEM));
        Assert.assertEquals(StreamingConstants.SPARK_SHUFFLE_PARTITIONS_DEFAULT,
                params.get(StreamingConstants.SPARK_SHUFFLE_PARTITIONS));
        Assert.assertEquals("false", params.get(StreamingConstants.STREAMING_RETRY_ENABLE));
    }
}
