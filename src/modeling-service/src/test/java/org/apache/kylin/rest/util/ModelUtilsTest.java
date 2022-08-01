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
package org.apache.kylin.rest.util;

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.constant.ModelAttributeEnum;
import org.apache.kylin.rest.response.NDataModelResponse;
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

import com.google.common.collect.Maps;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.response.SecondStorageInfo;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SecondStorageUtil.class })
public class ModelUtilsTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setUp() {
        SecurityContextHolder.getContext().setAuthentication(authentication);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
    }

    @After
    public void teardown() {
    }

    private void prepareSecondStorageInfo(List<SecondStorageInfo> secondStorageInfos) {
        val ssi = new SecondStorageInfo().setSecondStorageEnabled(true);
        Map<String, List<SecondStorageNode>> pairs = Maps.newHashMap();
        pairs.put("ssi", Arrays.asList(new SecondStorageNode(new Node())));
        ssi.setSecondStorageSize(1024).setSecondStorageNodes(pairs);
        secondStorageInfos.add(ssi);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable")).toReturn(Boolean.TRUE);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "setSecondStorageSizeInfo", List.class))
                .toReturn(secondStorageInfos);
    }

    @Test
    public void testComputeExpansionRate() {
        Assert.assertEquals("0", ModelUtils.computeExpansionRate(0, 1024));
        Assert.assertEquals("-1", ModelUtils.computeExpansionRate(1024, 0));

        String result = ModelUtils.computeExpansionRate(2048, 1024);
        Assert.assertEquals("200.00", result);
    }

    @Test
    public void testIsArgMatch() {
        Assert.assertTrue(ModelUtils.isArgMatch(null, true, "ADMIN"));
        Assert.assertTrue(ModelUtils.isArgMatch("ADMIN", true, "ADMIN"));
        Assert.assertTrue(ModelUtils.isArgMatch("ADMIN", false, "ADMIN"));
    }

    @Test
    public void testAddSecondStorageInfo() {
        prepareSecondStorageInfo(new ArrayList<>());
        val models = Arrays.asList((NDataModel) new NDataModelResponse());
        ModelUtils.addSecondStorageInfo("default", models);
        val dataModelResp = ((NDataModelResponse) models.get(0));
        Assert.assertEquals(1, dataModelResp.getSecondStorageNodes().size());
        Assert.assertEquals(1024, dataModelResp.getSecondStorageSize());
        Assert.assertEquals(true, dataModelResp.isSecondStorageEnabled());
    }

    @Test
    public void testGetFilterModels() {
        prepareSecondStorageInfo(new ArrayList<>());
        List<NDataModel> mockedModels = Lists.newArrayList();
        NDataModelResponse modelSpy4 = Mockito.spy(new NDataModelResponse(new NDataModel()));
        when(modelSpy4.isSecondStorageEnabled()).thenReturn(true);
        mockedModels.add(modelSpy4);
        val modelSets = ModelUtils.getFilteredModels("default", Arrays.asList(ModelAttributeEnum.SECOND_STORAGE),
                mockedModels);
        Assert.assertEquals(1, modelSets.size());
        val dataModelResp = ((NDataModelResponse) modelSets.iterator().next());
        Assert.assertEquals(1, dataModelResp.getSecondStorageNodes().size());
        Assert.assertEquals(1024, dataModelResp.getSecondStorageSize());
        Assert.assertEquals(true, dataModelResp.isSecondStorageEnabled());
    }
}
