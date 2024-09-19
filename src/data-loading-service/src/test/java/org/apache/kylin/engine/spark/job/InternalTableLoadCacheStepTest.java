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

import static org.apache.kylin.job.execution.JobTypeEnum.INTERNAL_TABLE_BUILD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.handler.InternalTableJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.job.service.InternalTableLoadingService;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.streaming.ReflectionUtils;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.InternalTableService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.utils.GlutenCacheUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.val;

@MetadataInfo
@OverwriteProp.OverwriteProps(value = {
        @OverwriteProp(key = "kylin.storage.columnar.spark-conf.spark.gluten.enabled", value = "true"),
        @OverwriteProp(key = "kylin.storage.columnar.spark-conf.spark.plugins", value = "GlutenPlugin"),
        @OverwriteProp(key = "kylin.internal-table-enabled", value = "true") })
class InternalTableLoadCacheStepTest extends InternalTableLoadingJobTest {
    @Test
    void doWork() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val internalTable = getInternalTableDesc(config);
        val cacheStep = getInternalTableLoadCacheStep(internalTable);

        {
            val executeResult = cacheStep.doWork(null);
            Assertions.assertTrue(executeResult.succeed());
            Assertions.assertEquals("succeed", executeResult.output());
        }

        {
            try (MockedStatic<GlutenCacheUtils> modelManagerMockedStatic = Mockito.mockStatic(GlutenCacheUtils.class)) {
                modelManagerMockedStatic
                        .when(() -> GlutenCacheUtils.generateCacheTableCommand(config, PROJECT,
                                internalTable.getIdentity(), "", Lists.newArrayList(), false))
                        .thenThrow(new KylinRuntimeException("test"));

                val executeResult = cacheStep.doWork(null);
                Assertions.assertFalse(executeResult.succeed());
                Assertions.assertFalse(executeResult.skip());
                Assertions.assertEquals(ExecuteResult.State.ERROR, executeResult.state());
                Assertions.assertNull(executeResult.output());
                Assertions.assertInstanceOf(KylinRuntimeException.class, executeResult.getThrowable());
                Assertions.assertEquals("test", executeResult.getThrowable().getMessage());
            }
        }
    }

    private InternalTableLoadCacheStep getInternalTableLoadCacheStep(InternalTableDesc internalTable) {
        val jobParam = new JobParam().withProject(PROJECT).withTable(internalTable.getIdentity()).withYarnQueue(null)
                .withJobTypeEnum(INTERNAL_TABLE_BUILD).withOwner("UT")
                .addExtParams(NBatchConstants.P_INCREMENTAL_BUILD, String.valueOf(false))
                .addExtParams(NBatchConstants.P_OUTPUT_MODE, String.valueOf(false))
                .addExtParams(NBatchConstants.P_START_DATE, "").addExtParams(NBatchConstants.P_END_DATE, "");
        val internalTableJobParam = new InternalTableJobHandler.InternalTableJobBuildParam(jobParam);
        val internalTableLoadingJob = InternalTableLoadingJob.create(internalTableJobParam);
        List<AbstractExecutable> tasks = internalTableLoadingJob.getTasks();
        return ((InternalTableLoadCacheStep) tasks.stream().filter(task -> task instanceof InternalTableLoadCacheStep)
                .findFirst().get());
    }

    private InternalTableDesc getInternalTableDesc(KylinConfig config) throws Exception {
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        val datePartitionFormat = "yyyy-MM-dd";
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn(datePartitionFormat);
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);
        return internalTable;
    }
}
