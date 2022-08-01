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

import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import lombok.val;
import reactor.core.publisher.Mono;

public class ProjectBasedLoadBalancerTest extends NLocalFileMetadataTestCase {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testChooseWithEmptyOwner() throws IOException {
        createHttpServletRequestMock("default2");
        exceptionRule.expect(KylinException.class);
        exceptionRule.expectMessage("System is trying to recover service. Please try again later.");
        Mono.from(new ProjectBasedLoadBalancer().choose()).block();
    }

    private void createHttpServletRequestMock(String project) throws IOException {
        HttpServletRequest request = Mockito.spy(HttpServletRequest.class);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
        when(request.getParameter("project")).thenReturn(project);

        val bodyJson = "{\"project\": \"" + project + "\"}";
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                bodyJson.getBytes(StandardCharsets.UTF_8));

        when(request.getInputStream()).thenReturn(new ServletInputStream() {
            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setReadListener(ReadListener listener) {

            }

            private boolean isFinished;

            @Override
            public int read() {
                int b = byteArrayInputStream.read();
                isFinished = b == -1;
                return b;
            }
        });
    }

    private void createTestProjectAndEpoch(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.createProject(project, "abcd", "", null);
        EpochManager.getInstance().updateEpochWithNotifier(project, false);

    }

    private void testChooseInternal() {

        String instance = AddressUtil.getLocalInstance();
        String[] split = instance.split(":");

        ServiceInstance server = Mono.from(new ProjectBasedLoadBalancer().choose()).block().getServer();
        Assert.assertEquals(split[0], server.getHost());
        Assert.assertEquals(Integer.parseInt(split[1]), server.getPort());
    }

    /**
     * test different project params that in request body
     * choose the given project' epoch
     * @param requestProject
     * @param project
     * @throws IOException
     */
    private void testRequestAndChooseOwner(String requestProject, String project) throws IOException {
        createHttpServletRequestMock(requestProject);
        createTestProjectAndEpoch(project);
        testChooseInternal();
    }

    @Test
    public void testChoose() throws IOException {

        {
            testRequestAndChooseOwner("TEst_ProJECT2", "test_project2");
        }

        {
            testRequestAndChooseOwner("TEST_PROJECT", "test_project");
        }
    }
}