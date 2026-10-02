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
package org.apache.kylin.rest.controller;

import java.util.Collections;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.AclGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.config.initialize.BroadcastListener;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fasterxml.jackson.databind.JsonNode;

public class BroadcastControllerTest extends NLocalFileMetadataTestCase {

    private BroadcastController controller;
    private BroadcastListener listener;

    @Before
    public void setUp() {
        createTestMetadata();
        listener = Mockito.mock(BroadcastListener.class);
        controller = new BroadcastController();
        ReflectionTestUtils.setField(controller, "localHandler", listener);
        SecurityContextHolder.clearContext();
        getTestConfig().setProperty("kylin.server.broadcast-token", "");
    }

    @After
    public void tearDown() {
        SecurityContextHolder.clearContext();
        cleanupTestMetadata();
    }

    @Test
    public void testUnauthenticatedAclBroadcastRejected() throws Exception {
        JsonNode event = aclGrantEvent();

        Assert.assertThrows(KylinException.class,
                () -> controller.broadcastReceive(event, new MockHttpServletRequest()));
        Mockito.verifyNoInteractions(listener);
    }

    @Test
    public void testGlobalAdminAclBroadcastAccepted() throws Exception {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        controller.broadcastReceive(aclGrantEvent(), new MockHttpServletRequest());

        Mockito.verify(listener).handle(Mockito.any(AclGrantEventNotifier.class));
    }

    @Test
    public void testBroadcastTokenAcceptedWithoutUserAuthentication() throws Exception {
        getTestConfig().setProperty("kylin.server.broadcast-token", "secret");
        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();

        mockMvc.perform(MockMvcRequestBuilders.post("/api/broadcast").contentType(MediaType.APPLICATION_JSON)
                .header(BroadcastEventReadyNotifier.BROADCAST_TOKEN_HEADER, "secret")
                .content(aclGrantEvent().toString())).andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(listener).handle(Mockito.any(AclGrantEventNotifier.class));
    }

    @Test
    public void testInvalidBroadcastTokenRejected() throws Exception {
        getTestConfig().setProperty("kylin.server.broadcast-token", "secret");
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(BroadcastEventReadyNotifier.BROADCAST_TOKEN_HEADER, "wrong");

        Assert.assertThrows(KylinException.class, () -> controller.broadcastReceive(aclGrantEvent(), request));
        Mockito.verifyNoInteractions(listener);
    }

    @Test
    public void testUnsupportedBroadcastTypeRejected() throws Exception {
        JsonNode event = JsonUtil.readValueAsTree(
                "{\"@class\":\"org.apache.kylin.common.persistence.event.ResourceDeleteEvent\"}");

        Assert.assertThrows(KylinException.class,
                () -> controller.broadcastReceive(event, new MockHttpServletRequest()));
        Mockito.verifyNoInteractions(listener);
    }

    private JsonNode aclGrantEvent() throws Exception {
        AclGrantEventNotifier notifier = new AclGrantEventNotifier("project-uuid",
                JsonUtil.writeValueAsString(Collections.emptyList()));
        return JsonUtil.readValueAsTree(JsonUtil.writeValueAsString(notifier));
    }
}
