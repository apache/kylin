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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.lang.reflect.Field;
import java.net.UnknownHostException;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.service.JobInfoService;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

public class JobDriverUIUtilTest extends NLocalFileMetadataTestCase {
    private MockHttpServletRequest request = new MockHttpServletRequest() {
    };
    private MockHttpServletResponse response = new MockHttpServletResponse();
    @Mock
    private final JobInfoService jobInfoService = Mockito.spy(JobInfoService.class);

    JobDriverUIUtil jobDriverUIUtil;

    @Test
    public void testProxy() throws Exception {
        createTestMetadata();
        MockedStatic<SparkUIUtil> mockedSparkUIUtil = Mockito.mockStatic(SparkUIUtil.class);
        jobDriverUIUtil = Mockito.spy(new JobDriverUIUtil());
        Field field = jobDriverUIUtil.getClass().getDeclaredField("jobInfoService");
        field.setAccessible(true);
        field.set(jobDriverUIUtil, jobInfoService);
        Mockito.when(jobInfoService.getOriginTrackUrlByProjectAndStepId(any(), any()))
                .thenReturn("http://testServer:4040");
        request.setRequestURI("/kylin/driver_ui/project/step/job");
        request.setMethod("GET");
        jobDriverUIUtil.proxy("project", "step", request, response);
        Mockito.when(jobInfoService.getOriginTrackUrlByProjectAndStepId(any(), any())).thenReturn(null);
        jobDriverUIUtil.proxy("project", "step", request, response);
        assert response.getContentAsString().contains("track url not generated yet");

        response = new MockHttpServletResponse();
        Mockito.when(jobInfoService.getOriginTrackUrlByProjectAndStepId(any(), any()))
                .thenReturn("http://testServer:4040");
        mockedSparkUIUtil.when(() -> SparkUIUtil.resendSparkUIRequest(any(), any(), eq("http://testServer:4040"),
                eq("/job"), eq("/kylin/driver_ui/project/step"))).thenThrow(UnknownHostException.class);
        jobDriverUIUtil.proxy("project", "step", request, response);
        assert response.getContentAsString().contains("track url invalid already");

        assert JobDriverUIUtil.getProxyUrl("project", "step").equals("/kylin/driver_ui/project/step");

        mockedSparkUIUtil.close();
    }
}
