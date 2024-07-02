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
import java.util.Map;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.job.handler.InternalTableJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.streaming.ReflectionUtils;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.InternalTableLoadingService;
import org.apache.kylin.rest.service.InternalTableService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.val;

@MetadataInfo
class InternalTableLoadingJobTest extends AbstractTestCase {
    protected static final String PROJECT = "default";
    protected static final String TABLE_INDENTITY = "DEFAULT.TEST_KYLIN_FACT";
    protected static final String DATE_COL = "CAL_DT";

    @Mock
    protected AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Spy
    protected InternalTableLoadingService internalTableLoadingService = Mockito.spy(new InternalTableLoadingService());
    @InjectMocks
    protected InternalTableService internalTableService = Mockito.spy(new InternalTableService());

    @InjectMocks
    protected TableService tableService = mock(TableService.class);

    @BeforeAll
    public static void beforeClass() {
        NLocalWithSparkSessionTestBase.beforeClass();
    }

    @AfterAll
    public static void afterClass() {
        NLocalWithSparkSessionTestBase.afterClass();
    }

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        SparkJobFactoryUtils.initJobFactory();
        overwriteSystemProp("kylin.source.provider.9", "org.apache.kylin.engine.spark.mockup.CsvSource");
        ReflectionUtils.setField(internalTableService, "aclEvaluate", aclEvaluate);
        ReflectionUtils.setField(internalTableService, "internalTableLoadingService", internalTableLoadingService);
    }

    protected InternalTableDesc getInternalTableDesc(KylinConfig config) throws Exception {
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

    @Test
    void isInternalTableSparkJob() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val internalTable = getInternalTableDesc(config);
        val jobParam = new JobParam().withProject(PROJECT).withTable(internalTable.getIdentity()).withYarnQueue(null)
                .withJobTypeEnum(INTERNAL_TABLE_BUILD).withOwner("UT")
                .addExtParams(NBatchConstants.P_INCREMENTAL_BUILD, String.valueOf(false))
                .addExtParams(NBatchConstants.P_OUTPUT_MODE, String.valueOf(false))
                .addExtParams(NBatchConstants.P_START_DATE, "").addExtParams(NBatchConstants.P_END_DATE, "");
        val internalTableJobParam = new InternalTableJobHandler.InternalTableJobBuildParam(jobParam);
        val internalTableLoadingJob = InternalTableLoadingJob.create(internalTableJobParam);
        Assertions.assertFalse(internalTableLoadingJob.isInternalTableSparkJob());
        internalTableLoadingJob.getTasks().forEach(task -> {
            if (task instanceof InternalTableLoadingStep) {
                Assertions.assertTrue(task.isInternalTableSparkJob());
            } else {
                Assertions.assertFalse(task.isInternalTableSparkJob());
            }
        });
    }
}
