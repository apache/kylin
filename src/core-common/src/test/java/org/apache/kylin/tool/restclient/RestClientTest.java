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
package org.apache.kylin.tool.restclient;

import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.apache.kylin.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class RestClientTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testNoDefaultBroadcastCredential() {
        Assert.assertEquals("", getTestConfig().getBroadcastToken());
    }

    @Test
    public void testNotifyAddsBroadcastToken() throws Exception {
        getTestConfig().setProperty("kylin.server.broadcast-token", "secret");
        RestClient restClient = new RestClient("localhost", 7070, null, null);
        DefaultHttpClient httpClient = Mockito.mock(DefaultHttpClient.class);
        restClient.client = httpClient;
        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        Mockito.when(response.getStatusLine())
                .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK"));
        Mockito.when(response.getEntity()).thenReturn(new StringEntity(""));
        Mockito.when(httpClient.execute(Mockito.any(HttpPost.class))).thenReturn(response);

        restClient.notify(new AuditLogBroadcastEventNotifier());

        ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);
        Mockito.verify(httpClient).execute(requestCaptor.capture());
        Assert.assertEquals("secret",
                requestCaptor.getValue().getFirstHeader(BroadcastEventReadyNotifier.BROADCAST_TOKEN_HEADER).getValue());
    }
}
