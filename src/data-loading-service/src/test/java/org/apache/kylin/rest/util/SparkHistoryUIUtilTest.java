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

import static org.apache.spark.deploy.history.HistoryServerBuilder.createHistoryServer;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.history.HistoryServer;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

public class SparkHistoryUIUtilTest extends NLocalFileMetadataTestCase {
    private MockHttpServletRequest request = new MockHttpServletRequest() {
    };
    private MockHttpServletResponse response = new MockHttpServletResponse();
    private HistoryServer historyServer;
    private SparkHistoryUIUtil sparkHistoryUIUtil;

    @Test
    public void testProxy() throws Exception {
        createTestMetadata();
        FileUtils.forceMkdir(new File("/tmp/spark-events"));
        historyServer = createHistoryServer(new SparkConf());
        sparkHistoryUIUtil = new SparkHistoryUIUtil();
        Field field = sparkHistoryUIUtil.getClass().getDeclaredField("historyServer");
        field.setAccessible(true);
        field.set(sparkHistoryUIUtil, historyServer);
        request.setRequestURI(SparkHistoryUIUtil.KYLIN_HISTORY_UI_BASE);
        request.setMethod("GET");
        sparkHistoryUIUtil.proxy(request, response);
        response.getContentType();
        assert response.getContentAsString().contains("href=\"" + SparkHistoryUIUtil.KYLIN_HISTORY_UI_BASE + "/");
        request.setRequestURI(SparkHistoryUIUtil.KYLIN_HISTORY_UI_BASE + "/history/app001");
        sparkHistoryUIUtil.proxy(request, response);
        assert SparkHistoryUIUtil.getHistoryTrackerUrl("app-0001")
                .equals(SparkHistoryUIUtil.PROXY_LOCATION_BASE + "/history/app-0001");

    }

    @Test
    public void test3xxProxy() throws Exception {
        HttpComponentsClientHttpRequestFactory mockFactory = Mockito.mock(HttpComponentsClientHttpRequestFactory.class);
        Field field = SparkUIUtil.class.getDeclaredField("factory");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, mockFactory);
        ClientHttpResponse mockClientHttpResponse = Mockito.mock(ClientHttpResponse.class);
        Mockito.when(mockClientHttpResponse.getStatusCode()).thenReturn(HttpStatus.FOUND);
        Mockito.when(mockClientHttpResponse.getHeaders()).thenReturn(new HttpHeaders());

        ClientHttpRequest mockRequest = Mockito.mock(ClientHttpRequest.class);
        Mockito.when(mockRequest.getHeaders()).thenReturn(new HttpHeaders());
        Mockito.when(mockRequest.execute()).thenReturn(mockClientHttpResponse);
        Mockito.when(mockFactory.createRequest(Mockito.any(), Mockito.any())).thenReturn(mockRequest);
        request.setRequestURI(SparkHistoryUIUtil.KYLIN_HISTORY_UI_BASE);
        request.setMethod("GET");
        response.setWriterAccessAllowed(true);
        SparkUIUtil.resendSparkUIRequest(request, response, "http://localhost:18080", "history_server", "proxy");
        Mockito.verify(mockRequest, Mockito.times(6)).execute();

    }
}
