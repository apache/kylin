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

package org.apache.kylin.job.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JobUtilTest {

    private MockedStatic<KylinConfig> kylinConfigMockedStatic;
    private MockedStatic<NTableMetadataManager> tableMetadataManagerMockedStatic;
    private MockedStatic<NExecutableManager> executableManagerMockedStatic;
    private MockedStatic<NDataflowManager> dataflowManagerMockedStatic;
    private MockedStatic<NDataModelManager> dataModelManagerMockedStatic;

    @Mock
    private NTableMetadataManager tableMetadataManager;
    @Mock
    private NExecutableManager executableManager;
    @Mock
    private NDataflowManager dataflowManager;
    @Mock
    private NDataModelManager dataModelManager;

    @Before
    public void before() {
        kylinConfigMockedStatic = mockStatic(KylinConfig.class);
        kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(null);

        tableMetadataManagerMockedStatic = mockStatic(NTableMetadataManager.class);
        tableMetadataManagerMockedStatic.when(() -> NTableMetadataManager.getInstance(any(), any()))
                .thenReturn(tableMetadataManager);

        executableManagerMockedStatic = mockStatic(NExecutableManager.class);
        executableManagerMockedStatic.when(() -> NExecutableManager.getInstance(any(), any()))
                .thenReturn(executableManager);

        dataflowManagerMockedStatic = mockStatic(NDataflowManager.class);
        dataflowManagerMockedStatic.when(() -> NDataflowManager.getInstance(any(), any())).thenReturn(dataflowManager);

        dataModelManagerMockedStatic = mockStatic(NDataModelManager.class);
        dataModelManagerMockedStatic.when(() -> NDataModelManager.getInstance(any(), any()))
                .thenReturn(dataModelManager);
    }

    @After
    public void after() {
        kylinConfigMockedStatic.close();
        tableMetadataManagerMockedStatic.close();
        executableManagerMockedStatic.close();
        dataflowManagerMockedStatic.close();
        dataModelManagerMockedStatic.close();
    }

    @Test
    public void testDeduceTargetSubject_JobType_TableSampling() {
        ExecutablePO executablePO = new ExecutablePO();
        executablePO.setJobType(JobTypeEnum.TABLE_SAMPLING);
        Map<String, String> params = new HashMap<>();
        params.put(NBatchConstants.P_TABLE_NAME, "SSB.CUSTOMER");
        executablePO.setParams(params);

        TableDesc tableDesc = mock(TableDesc.class);

        // tableDesc == null
        when(tableMetadataManager.getTableDesc(any())).thenReturn(null);
        assertNull(JobUtil.deduceTargetSubject(executablePO));

        // tableDesc != null
        when(tableMetadataManager.getTableDesc(any())).thenReturn(tableDesc);
        assertEquals("SSB.CUSTOMER", JobUtil.deduceTargetSubject(executablePO));
    }

    @Test
    public void testDeduceTargetSubject_JobType_Snapshot() {
        ExecutablePO executablePO = new ExecutablePO();
        executablePO.setJobType(JobTypeEnum.SNAPSHOT_BUILD);
        Map<String, String> params = new HashMap<>();
        params.put(NBatchConstants.P_TABLE_NAME, "SSB.CUSTOMER");
        executablePO.setParams(params);

        Output output = mock(DefaultOutput.class);
        TableDesc tableDesc = mock(TableDesc.class);

        // state.isFinalState() == true && tableDesc == null
        when(executableManager.getOutput(any())).thenReturn(output);
        when(output.getState()).thenReturn(ExecutableState.SUCCEED);
        when(tableMetadataManager.getTableDesc(any())).thenReturn(null);
        assertNull(JobUtil.deduceTargetSubject(executablePO));

        // state.isFinalState() == true && tableDesc.getLastSnapshotPath() == null
        when(output.getState()).thenReturn(ExecutableState.SUCCEED);
        when(tableMetadataManager.getTableDesc(any())).thenReturn(tableDesc);
        when(tableDesc.getLastSnapshotPath()).thenReturn(null);
        assertNull(JobUtil.deduceTargetSubject(executablePO));

        // state.isFinalState() == false
        when(output.getState()).thenReturn(ExecutableState.RUNNING);
        assertEquals("SSB.CUSTOMER", JobUtil.deduceTargetSubject(executablePO));

        // state.isFinalState() == true && tableDesc != null && tableDesc.getLastSnapshotPath() != null
        when(output.getState()).thenReturn(ExecutableState.SUCCEED);
        when(tableMetadataManager.getTableDesc(any())).thenReturn(tableDesc);
        when(tableDesc.getLastSnapshotPath()).thenReturn("/path/to/last/snapshot");
        assertEquals("SSB.CUSTOMER", JobUtil.deduceTargetSubject(executablePO));
    }

    @Test
    public void testDeduceTargetSubject_JobType_SecondStorage() {
        ExecutablePO executablePO = new ExecutablePO();
        executablePO.setJobType(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN);
        executablePO.setProject("project_01");

        assertEquals("project_01", JobUtil.deduceTargetSubject(executablePO));
    }

    @Test
    public void testGetModelAlias() {
        ExecutablePO executablePO = new ExecutablePO();
        executablePO.setTargetModel("SSB.CUSTOMER");
        NDataModel dataModelDesc = mock(NDataModel.class);
        NDataModel dataModelWithoutInit = mock(NDataModel.class);

        // dataModelDesc == null
        when(dataModelManager.getDataModelDesc(any())).thenReturn(null);
        assertNull(JobUtil.getModelAlias(executablePO));

        // dataModelDesc != null && dataModelManager.isModelBroken(targetModel) == true
        when(dataModelManager.getDataModelDesc(any())).thenReturn(dataModelDesc);
        when(dataModelManager.isModelBroken(any())).thenReturn(true);
        when(dataModelManager.getDataModelDescWithoutInit(any())).thenReturn(dataModelWithoutInit);
        JobUtil.getModelAlias(executablePO);
        verify(dataModelManager).getDataModelDescWithoutInit(any());

        // dataModelDesc != null && dataModelManager.isModelBroken(targetModel) == false
        when(dataModelManager.getDataModelDesc(any())).thenReturn(dataModelDesc);
        when(dataModelManager.isModelBroken(any())).thenReturn(false);
        JobUtil.getModelAlias(executablePO);
        verify(dataModelDesc).getAlias();
    }
}
