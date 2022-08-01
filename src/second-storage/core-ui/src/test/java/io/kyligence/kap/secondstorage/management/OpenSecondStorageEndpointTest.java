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

package io.kyligence.kap.secondstorage.management;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.response.NModelDescResponse;
import org.apache.kylin.rest.service.ModelService;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import lombok.val;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;

public class OpenSecondStorageEndpointTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    @Mock
    private SecondStorageEndpoint secondStorageEndpoint;

    @InjectMocks
    private final OpenSecondStorageEndpoint openSecondStorageEndpoint = Mockito.spy(new OpenSecondStorageEndpoint());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(openSecondStorageEndpoint)
                .defaultRequest(MockMvcRequestBuilders.get("/api/storage/segments"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void loadStorage() throws Exception {
        val model = new NModelDescResponse();
        model.setUuid("11111111");
        Mockito.when(modelService.getModelDesc("test", "default"))
                .thenReturn(model);
        Mockito.when(modelService.convertSegmentIdWithName("test", "default", new String[]{"seg1", "seg2"}, new String[]{}))
                .thenReturn(new String[]{"seg1", "seg2"});

        StorageRequest storageRequest = new StorageRequest();
        storageRequest.setModelName("test");
        storageRequest.setProject("default");
        storageRequest.setSegmentIds(Lists.asList("seg1", new String[]{"seg2"}));
        val param = JsonUtil.writeValueAsString(storageRequest);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/storage/segments").content(param)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Mockito.verify(openSecondStorageEndpoint).loadStorage(Mockito.any(StorageRequest.class));
    }

    @Test
    public void testOpenCleanEmptySegment() throws Exception {
        val request = new StorageRequest();
        request.setProject("default");
        request.setModelName("test");
        try {
            openSecondStorageEndpoint.convertSegmentIdWithName(request);
        } catch (KylinException exception) {
            Assert.assertEquals(SEGMENT_EMPTY_PARAMETER.getErrorCode().getCode(), exception.getErrorCode().getCodeString());
            return;
        }
        Assert.fail();
    }
}
