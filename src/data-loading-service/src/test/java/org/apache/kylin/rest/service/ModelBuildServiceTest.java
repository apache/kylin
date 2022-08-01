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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ModelBuildServiceTest {

    @Spy
    private ModelBuildService modelBuildService;

    @Test
    public void testCheckMultiPartitionBuildParam() {
        NDataModel model = mock(NDataModel.class);
        IncrementBuildSegmentParams params = mock(IncrementBuildSegmentParams.class);

        // Not a multi-partition model, no need check
        when(model.isMultiPartitionModel()).thenReturn(false);
        modelBuildService.checkMultiPartitionBuildParam(model, params);

        // Test throwing JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY
        when(model.isMultiPartitionModel()).thenReturn(true);
        when(params.isNeedBuild()).thenReturn(true);
        when(params.getMultiPartitionValues()).thenReturn(new ArrayList<>());
        try {
            modelBuildService.checkMultiPartitionBuildParam(model, params);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY.getCodeMsg(), e.getLocalizedMessage());
        }

        // Test throwing 1st JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON
        when(model.isMultiPartitionModel()).thenReturn(true);
        when(params.isNeedBuild()).thenReturn(false);
        List<String[]> partitionValues = new ArrayList<>();
        partitionValues.add(new String[0]);
        when(params.getMultiPartitionValues()).thenReturn(partitionValues);
        try {
            modelBuildService.checkMultiPartitionBuildParam(model, params);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON.getCodeMsg(), e.getLocalizedMessage());
        }

        // Test throwing 2nd JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON
        when(model.isMultiPartitionModel()).thenReturn(true);
        when(params.isNeedBuild()).thenReturn(true);
        partitionValues = new ArrayList<>();
        partitionValues.add(new String[] { "p1" });
        when(params.getMultiPartitionValues()).thenReturn(partitionValues);
        // mock column size
        LinkedList<String> columns = mock(LinkedList.class);
        MultiPartitionDesc multiPartitionDesc = mock(MultiPartitionDesc.class);
        when(model.getMultiPartitionDesc()).thenReturn(multiPartitionDesc);
        when(multiPartitionDesc.getColumns()).thenReturn(columns);
        when(columns.size()).thenReturn(2);
        try {
            modelBuildService.checkMultiPartitionBuildParam(model, params);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON.getCodeMsg(), e.getLocalizedMessage());
        }
    }

}
