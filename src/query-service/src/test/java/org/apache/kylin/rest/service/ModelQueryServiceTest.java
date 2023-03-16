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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.constant.ModelAttributeEnum;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.service.params.ModelQueryParams;
import org.apache.kylin.rest.util.ModelTriple;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SecondStorageUtil.class, UserGroupInformation.class })
public class ModelQueryServiceTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    private ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testFilterModelOfBatch() {
        val mt1 = new ModelTriple(new NDataflow(), new NDataModelResponse());
        mt1.getDataModel().setModelType(NDataModel.ModelType.BATCH);
        List models = Arrays.asList(mt1);

        List<ModelAttributeEnum> modelAttributeSet1 = Lists.newArrayList(ModelAttributeEnum.BATCH);
        ModelQueryParams modelQueryParams = new ModelQueryParams("", null, true, "default", null, null, "", 0, 10, "",
                true, null, modelAttributeSet1, null, null, true, false);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable")).toReturn(Boolean.TRUE);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isModelEnable")).toReturn(Boolean.FALSE);

        List<ModelTriple> filteredModels1 = modelQueryService.filterModels(models, modelQueryParams);
        Assert.assertEquals(1, filteredModels1.size());
    }

    @Test
    public void testFilterModelOfStreaming() {
        val mt1 = new ModelTriple(new NDataflow(), new NDataModelResponse());
        mt1.getDataModel().setModelType(NDataModel.ModelType.STREAMING);
        val mt2 = new ModelTriple(new NDataflow(), new NDataModelResponse());
        mt2.getDataModel().setModelType(NDataModel.ModelType.HYBRID);
        List models = Arrays.asList(mt1, mt2);

        List<ModelAttributeEnum> modelAttributeSet1 = Lists.newArrayList(ModelAttributeEnum.STREAMING,
                ModelAttributeEnum.HYBRID);
        ModelQueryParams modelQueryParams = new ModelQueryParams("", null, true, "streaming_test", null, null, "", 0,
                10, "", true, null, modelAttributeSet1, null, null, true, false);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable")).toReturn(Boolean.TRUE);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isModelEnable")).toReturn(Boolean.FALSE);

        List<ModelTriple> filteredModels1 = modelQueryService.filterModels(models, modelQueryParams);
        Assert.assertEquals(2, filteredModels1.size());
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        List<ModelTriple> filteredModels2 = modelQueryService.filterModels(models, modelQueryParams);
        Assert.assertEquals(0, filteredModels2.size());
    }

    @Test
    public void testFilterModelWithSecondStorage() {
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable")).toReturn(Boolean.TRUE);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isModelEnable")).toReturn(Boolean.TRUE);

        val mt1 = new ModelTriple(new NDataflow(), new NDataModelResponse());
        mt1.getDataModel().setModelType(NDataModel.ModelType.BATCH);

        List models = Arrays.asList(mt1);

        List<ModelAttributeEnum> modelAttributeSet1 = Lists.newArrayList(ModelAttributeEnum.BATCH,
                ModelAttributeEnum.SECOND_STORAGE);
        ModelQueryParams modelQueryParams = new ModelQueryParams("", null, true, "default", null, null, "", 0, 10, "",
                true, null, modelAttributeSet1, null, null, true, false);
        List<ModelTriple> filteredModels1 = modelQueryService.filterModels(models, modelQueryParams);
        Assert.assertEquals(1, filteredModels1.size());

        List<ModelAttributeEnum> modelAttributeSet2 = Lists.newArrayList(ModelAttributeEnum.SECOND_STORAGE);
        ModelQueryParams modelQueryParams2 = new ModelQueryParams("", null, true, "default", null, null, "", 0, 10, "",
                true, null, modelAttributeSet2, null, null, true, false);
        List<ModelTriple> filteredModels2 = modelQueryService.filterModels(models, modelQueryParams2);
        Assert.assertEquals(1, filteredModels2.size());
    }

    @Test
    public void testFilterModelAttribute() {
        Set<ModelAttributeEnum> modelAttributeSet1 = Sets.newHashSet(ModelAttributeEnum.BATCH,
                ModelAttributeEnum.SECOND_STORAGE);
        val mt1 = new ModelTriple(new NDataflow(), new NDataModelResponse());
        mt1.getDataModel().setModelType(NDataModel.ModelType.UNKNOWN);
        Assert.assertFalse(modelQueryService.filterModelAttribute(mt1, modelAttributeSet1, false));
    }
}
