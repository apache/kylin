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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.utils.GlutenCacheUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.sparkproject.guava.collect.Sets;

import lombok.val;

@MetadataInfo
@OverwriteProp.OverwriteProps(value = {
        @OverwriteProp(key = "kylin.storage.columnar.spark-conf.spark.gluten.enabled", value = "true"),
        @OverwriteProp(key = "kylin.storage.columnar.spark-conf.spark.plugins", value = "GlutenPlugin") })
class ModelIndexLoadCacheStepTest {
    static final String PROJECT = "default";
    static final String PROJECT_V3 = "storage_v3_test";
    private KylinConfig config;

    @BeforeEach
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();

        config = TestUtils.getTestConfig();
        JobContextUtil.getJobContext(config);
    }

    @AfterEach
    public void tearDown() throws Exception {
        await().untilAsserted(
                () -> assertFalse(JobContextUtil.getJobContext(config).getJobScheduler().hasRunningJob()));
        JobContextUtil.cleanUp();
    }

    private void cleanupSegments(String dfName, String project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dsMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow df = dsMgr.getDataflow(dfName);
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            return dsMgr.updateDataflow(update);
        }, project);
    }

    @Test
    void doWorkV1Model() throws Exception {
        val execMgr = ExecutableManager.getInstance(config, PROJECT);
        val dsMgr = NDataflowManager.getInstance(config, PROJECT);
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

        cleanupSegments(modelId, PROJECT);
        val df = dsMgr.getDataflow(modelId);
        assertEquals(0, df.getSegments().size());
        val oneSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        val layouts = df.getIndexPlan().getAllLayouts();
        val job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        execMgr.addJob(job);
        val cacheStep = (ModelIndexLoadCacheStep) job.getTasks().stream()
                .filter(task -> task instanceof ModelIndexLoadCacheStep).findFirst().get();

        {
            val executeResult = cacheStep.doWork(null);
            assertTrue(executeResult.succeed());
            assertEquals("succeed", executeResult.output());
        }

        {
            try (MockedStatic<GlutenCacheUtils> modelManagerMockedStatic = Mockito.mockStatic(GlutenCacheUtils.class)) {
                modelManagerMockedStatic.when(() -> GlutenCacheUtils.generateModelCacheCommands(config, PROJECT,
                        modelId, Lists.newArrayList(oneSeg.getId()))).thenThrow(new KylinRuntimeException("test"));

                val executeResult = cacheStep.doWork(null);
                assertFalse(executeResult.succeed());
                assertFalse(executeResult.skip());
                assertEquals(ExecuteResult.State.ERROR, executeResult.state());
                assertNull(executeResult.output());
                assertInstanceOf(KylinRuntimeException.class, executeResult.getThrowable());
                assertEquals("test", executeResult.getThrowable().getMessage());
            }
        }
    }

    @Test
    void doWorkV3Model() throws Exception {
        val execMgr = ExecutableManager.getInstance(config, PROJECT_V3);
        val dsMgr = NDataflowManager.getInstance(config, PROJECT_V3);
        val modelId = "7d840904-7b34-4edd-aabd-79df992ef32e";

        cleanupSegments(modelId, PROJECT_V3);
        val df = dsMgr.getDataflow(modelId);
        assertEquals(0, df.getSegments().size());
        val oneSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        val layouts = df.getIndexPlan().getAllLayouts();
        val job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        execMgr.addJob(job);
        val cacheStep = (ModelIndexLoadCacheStep) job.getTasks().stream()
                .filter(task -> task instanceof ModelIndexLoadCacheStep).findFirst().get();

        {
            val executeResult = cacheStep.doWork(null);
            assertTrue(executeResult.succeed());
            assertEquals("succeed", executeResult.output());
        }

        {
            try (MockedStatic<GlutenCacheUtils> modelManagerMockedStatic = Mockito.mockStatic(GlutenCacheUtils.class)) {
                modelManagerMockedStatic.when(() -> GlutenCacheUtils.generateModelCacheCommands(config, PROJECT_V3,
                        modelId, Lists.newArrayList(oneSeg.getId()))).thenThrow(new KylinRuntimeException("test"));
                val executeResult = cacheStep.doWork(null);
                assertFalse(executeResult.succeed());
                assertFalse(executeResult.skip());
                assertEquals(ExecuteResult.State.ERROR, executeResult.state());
                assertNull(executeResult.output());
                assertInstanceOf(KylinRuntimeException.class, executeResult.getThrowable());
                assertEquals("test", executeResult.getThrowable().getMessage());
            }
        }
    }
}
