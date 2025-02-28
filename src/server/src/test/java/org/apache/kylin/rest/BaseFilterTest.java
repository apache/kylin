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

package org.apache.kylin.rest;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_EARLY_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_WITHOUT_RESOURCE_GROUP;
import static org.apache.kylin.common.exception.ServerErrorCode.SYSTEM_IS_RECOVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import org.apache.kylin.common.exception.ErrorCodeSupplier;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletResponse;

import lombok.val;
import lombok.var;

@MetadataInfo
class BaseFilterTest {

    @Test
    void getKylinException() {
        try (val mockStatic = Mockito.mockStatic(ResourceGroupManager.class)) {
            val rgManager = Mockito.mock(ResourceGroupManager.class);
            mockStatic.when(() -> ResourceGroupManager.getInstance(any())).thenReturn(rgManager);
            val message = Message.getInstance();
            getKylinException(rgManager, true, false, PROJECT_WITHOUT_RESOURCE_GROUP,
                    message.getProjectWithoutResourceGroup());
            getKylinException(rgManager, false, false, SYSTEM_IS_RECOVER, message.getLeadersHandleOver());
            getKylinException(rgManager, true, true, SYSTEM_IS_RECOVER, message.getLeadersHandleOver());
            getKylinException(rgManager, false, true, SYSTEM_IS_RECOVER, message.getLeadersHandleOver());
        }
    }

    private void getKylinException(ResourceGroupManager rgManager, boolean isResourceGroupEnabled,
            boolean isProjectBindToResourceGroup, ErrorCodeSupplier errorCodeSupplier, String messageString) {
        Mockito.when(rgManager.isResourceGroupEnabled()).thenReturn(isResourceGroupEnabled);
        Mockito.when(rgManager.isProjectBindToResourceGroup(anyString())).thenReturn(isProjectBindToResourceGroup);
        val baseFilter = Mockito.spy(BaseFilter.class);
        val result = baseFilter.getKylinException("default", Message.getInstance());
        assertEquals(errorCodeSupplier.toErrorCode().getCodeString(), result.getErrorCodeString());
        assertEquals(messageString, result.getMessage());
    }

    @Test
    void setResponseHeaders() {
        val baseFilter = Mockito.spy(BaseFilter.class);
        val httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.ACCEPT, HTTP_VND_APACHE_KYLIN_V2_JSON);
        httpHeaders.set(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_EARLY_JSON);
        var res = new MockHttpServletResponse();
        baseFilter.setResponseHeaders(httpHeaders, res);
        assertEquals(HTTP_VND_APACHE_KYLIN_EARLY_JSON, res.getHeader(HttpHeaders.CONTENT_TYPE));
        assertEquals(HTTP_VND_APACHE_KYLIN_V2_JSON, res.getHeader(HttpHeaders.ACCEPT));

        httpHeaders.set(HttpHeaders.TRANSFER_ENCODING, "test");
        res = new MockHttpServletResponse();
        baseFilter.setResponseHeaders(httpHeaders, res);
        assertEquals(HTTP_VND_APACHE_KYLIN_EARLY_JSON, res.getHeader(HttpHeaders.CONTENT_TYPE));
        assertEquals(HTTP_VND_APACHE_KYLIN_V2_JSON, res.getHeader(HttpHeaders.ACCEPT));
        assertNull(res.getHeader(HttpHeaders.TRANSFER_ENCODING));
    }
}
