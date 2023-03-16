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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;

import io.kyligence.kap.secondstorage.management.request.ModelModifyRequest;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.NModelDescResponse;
import org.apache.kylin.rest.service.ModelService;
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

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import lombok.val;

public class OpenSecondStorageEndpointTest extends NLocalFileMetadataTestCase {
    private static final String NULLABLE_STRING = "Nullable(String)";
    private static final String LOW_CARDINALITY_STRING = "LowCardinality(Nullable(String))";
    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    private SecondStorageService secondStorageService = new SecondStorageService();

    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    @InjectMocks
    private final OpenSecondStorageEndpoint openSecondStorageEndpoint = Mockito.spy(new OpenSecondStorageEndpoint());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        openSecondStorageEndpoint.setSecondStorageService(secondStorageService);
        openSecondStorageEndpoint.setSecondStorageEndpoint(secondStorageEndpoint);
        openSecondStorageEndpoint.setModelService(modelService);
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

    @Test
    public void testEnableStorageException() throws Exception{
        val request = new ModelEnableRequest();
        request.setProject("default");
        request.setModelName("test");
        request.setEnabled(true);
        try {
            openSecondStorageEndpoint.enableStorage(request);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(MODEL_NAME_NOT_EXIST, e.getErrorCodeProducer());
        }

        val req = new ModelEnableRequest();
        req.setProject("default");
        req.setModelName("test");
        try {
            openSecondStorageEndpoint.enableStorage(req);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, e.getErrorCodeProducer());
        }
    }

    @Test
    public void testModifyColumnException() {
        val request = new ModelModifyRequest();
        request.setProject("default");
        request.setModelName("test");
        try {
            openSecondStorageEndpoint.modifyColumn(request);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(MODEL_NAME_NOT_EXIST, e.getErrorCodeProducer());
        }

        val req = new ModelModifyRequest();
        req.setProject("defaulT");
        req.setModelName("test_bank");
        req.setDatatype("");
        try {
            openSecondStorageEndpoint.modifyColumn(req);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("Please enter the value for the parameter 'datatype'.", e.getMessage());
        }

        val req1 = new ModelModifyRequest();
        req1.setProject("default");
        req1.setModelName("test_bank");
        req1.setDatatype("test_bank");
        try {
            openSecondStorageEndpoint.modifyColumn(req1);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("The datatype is invalid. Only support LowCardinality(Nullable(String)) or Nullable(String) at the moment.", e.getMessage());
        }

        val req2 = new ModelModifyRequest();
        req2.setProject("default");
        req2.setModelName("test_bank");
        req2.setDatatype(LOW_CARDINALITY_STRING);
        try {
            openSecondStorageEndpoint.modifyColumn(req2);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("Please enter the value for the parameter 'column'.", e.getMessage());
        }

        val req3 = new ModelModifyRequest();
        req3.setProject("default");
        req3.setModelName("test_bank");
        req3.setDatatype(NULLABLE_STRING);
        try {
            openSecondStorageEndpoint.modifyColumn(req3);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
        }

        val req4 = new ModelModifyRequest();
        req4.setProject("default");
        req4.setModelName("test_bank");
        req4.setDatatype(LOW_CARDINALITY_STRING);
        req4.setColumn("");
        try {
            openSecondStorageEndpoint.modifyColumn(req4);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
        }

        val req5 = new ModelModifyRequest();
        req5.setProject("defaulT");
        req5.setModelName("test_bank");
        req5.setDatatype(LOW_CARDINALITY_STRING);
        req5.setColumn("LO_test");
        try {
            openSecondStorageEndpoint.modifyColumn(req5);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
        }
    }
}
