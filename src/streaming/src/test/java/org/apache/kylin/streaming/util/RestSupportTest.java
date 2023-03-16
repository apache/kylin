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
package org.apache.kylin.streaming.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.streaming.request.StreamingJobUpdateRequest;
import org.apache.kylin.streaming.rest.RestSupport;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import lombok.val;

public class RestSupportTest extends NLocalFileMetadataTestCase {

    static boolean assertMeet = false;
    static CountDownLatch latch;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testRestSupport() {
        val config = getTestConfig();
        val rest = new RestSupport(config);
        val baseUrl = ReflectionUtils.getField(rest, "baseUrl");
        Assert.assertEquals("http://127.0.0.1:7070/kylin/api", baseUrl);
    }

    @Test
    public void testHttpPost() {
        List<Integer> ports = Lists.newArrayList(50000, 51000, 52000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    val request = new StreamingJobUpdateRequest();
                    server.createContext("/test", new NormalModeHandler(request));
                    server.start();

                    val rest = new RestSupport("http://localhost:" + port + "/test/");
                    val restResp = rest.execute(rest.createHttpPost("status"), new StreamingJobUpdateRequest());
                    Assert.assertNotNull(restResp);
                    Assert.assertEquals("0", restResp.getCode());
                    Assert.assertEquals("false", restResp.getData());
                    Assert.assertEquals("", restResp.getMsg());

                    val maintenanceMode = rest.isMaintenanceMode();
                    Assert.assertFalse(maintenanceMode);

                    val checkMaintenanceMode = rest.checkMaintenceMode();
                    Assert.assertFalse(checkMaintenanceMode);
                    rest.close();
                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testHttpPut() {
        List<Integer> ports = Lists.newArrayList(50000, 51000, 52000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    val request = new StreamingJobUpdateRequest();
                    server.createContext("/test", new NormalModeHandler(request));
                    server.start();

                    val rest = new RestSupport("http://localhost:" + port + "/test/");
                    val restResp = rest.execute(rest.createHttpPut("status"), new StreamingJobUpdateRequest());
                    Assert.assertNotNull(restResp);
                    Assert.assertEquals("0", restResp.getCode());
                    Assert.assertEquals("false", restResp.getData());
                    Assert.assertEquals("", restResp.getMsg());

                    val maintenanceMode = rest.isMaintenanceMode();
                    Assert.assertFalse(maintenanceMode);

                    val checkMaintenanceMode = rest.checkMaintenceMode();
                    Assert.assertFalse(checkMaintenanceMode);
                    rest.close();
                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testHttpGet() {
        List<Integer> ports = Lists.newArrayList(50000, 51000, 52000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    val request = new StreamingJobUpdateRequest();
                    server.createContext("/test", new NormalModeHandler(request));
                    server.start();

                    val rest = new RestSupport("http://localhost:" + port + "/test/");
                    val restResp = rest.execute(rest.createHttpGet("status"), null);
                    Assert.assertNotNull(restResp);
                    Assert.assertEquals("0", restResp.getCode());
                    Assert.assertEquals("false", restResp.getData());
                    Assert.assertEquals("", restResp.getMsg());

                    val maintenanceMode = rest.isMaintenanceMode();
                    Assert.assertFalse(maintenanceMode);

                    val checkMaintenanceMode = rest.checkMaintenceMode();
                    Assert.assertFalse(checkMaintenanceMode);
                    rest.close();
                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testMaintenanceMode() {
        List<Integer> ports = Lists.newArrayList(50000, 51000, 52000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    val request = new StreamingJobUpdateRequest();
                    server.createContext("/test", new MaintenanceModeHandler(request, 10));
                    server.start();

                    val rest = new RestSupport("http://localhost:" + port + "/test/");
                    val restResp = rest.execute(rest.createHttpPost("status"), new StreamingJobUpdateRequest());
                    Assert.assertNotNull(restResp);
                    Assert.assertEquals("0", restResp.getCode());
                    Assert.assertEquals("true", restResp.getData());
                    Assert.assertEquals("maintenance", restResp.getMsg());

                    val maintenanceMode = rest.isMaintenanceMode();
                    Assert.assertTrue(maintenanceMode);
                    rest.close();

                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testCheckMaintenanceMode() {
        val rest = Mockito.mock(RestSupport.class);
        Mockito.when(rest.isMaintenanceMode()).thenReturn(false);
        Assert.assertFalse(rest.checkMaintenceMode());
    }

    @Test
    public void testIsMaintenanceMode() {
        val rest = Mockito.mock(RestSupport.class);
        Mockito.when(rest.isMaintenanceMode()).thenReturn(false);
        Assert.assertFalse(rest.isMaintenanceMode());
    }

    static class NormalModeHandler implements HttpHandler {
        Object req;

        public NormalModeHandler(Object req) {
            this.req = req;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                InputStream in = httpExchange.getRequestBody();
                String s = IOUtils.toString(in);
                if (s.equals(JsonUtil.writeValueAsString(req))) {
                    assertMeet = true;
                }

            } finally {

                httpExchange.sendResponseHeaders(HttpStatus.SC_OK, 0L);
                httpExchange.getResponseBody()
                        .write(JsonUtil.writeValueAsString(new RestResponse<String>("0", "false", "")).getBytes());
                httpExchange.close();
                latch.countDown();
            }
        }
    }

    static class MaintenanceModeHandler implements HttpHandler {
        Object req;
        int mtmCnt;

        public MaintenanceModeHandler(Object req, int mtmCnt) {
            this.req = req;
            this.mtmCnt = mtmCnt;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                InputStream in = httpExchange.getRequestBody();
                String s = IOUtils.toString(in);
                if (s.equals(JsonUtil.writeValueAsString(req))) {
                    assertMeet = true;
                }
            } finally {
                httpExchange.sendResponseHeaders(HttpStatus.SC_OK, 0L);
                if (mtmCnt > 0) {
                    httpExchange.getResponseBody().write(JsonUtil
                            .writeValueAsString(new RestResponse<String>("0", "true", "maintenance")).getBytes());
                    mtmCnt--;
                } else {
                    httpExchange.getResponseBody().write(JsonUtil
                            .writeValueAsString(new RestResponse<String>("0", "false", "maintenance")).getBytes());
                }
                httpExchange.close();
                latch.countDown();
            }
        }
    }
}
