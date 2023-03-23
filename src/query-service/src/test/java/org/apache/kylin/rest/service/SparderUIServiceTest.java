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

package org.apache.kylin.rest.service;

import static org.apache.commons.net.util.Base64.encodeBase64;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.Charset;
import java.util.stream.Collectors;

import javax.servlet.http.Cookie;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.streaming.ReflectionUtils;
import org.apache.kylin.rest.util.SparderUIUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.client.RestTemplate;

import lombok.val;
import lombok.var;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HttpMethod.class, KylinConfig.class })
public class SparderUIServiceTest {
    @InjectMocks
    private SparderUIService sparderUIService = Mockito.spy(SparderUIService.class);
    @Mock
    private RouteService routeService = Mockito.mock(RouteService.class);
    @Mock
    private RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    @Mock
    private KylinConfig kylinConfig = Mockito.mock(KylinConfig.class);
    private SparderUIUtil sparderUIUtil;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(HttpMethod.class, KylinConfig.class);
        PowerMockito.when(HttpMethod.valueOf(ArgumentMatchers.anyString())).thenAnswer(invocation -> HttpMethod.GET);
        PowerMockito.when(KylinConfig.getInstanceFromEnv()).thenAnswer(invocation -> kylinConfig);
        Mockito.when(kylinConfig.getUIProxyLocation()).thenReturn("/kylin");

        sparderUIUtil = Mockito.mock(SparderUIUtil.class);
        ReflectionUtils.setField(sparderUIService, "routeService", routeService);
        ReflectionUtils.setField(sparderUIService, "restTemplate", restTemplate);
        ReflectionUtils.setField(sparderUIService, "sparderUIUtil", sparderUIUtil);

        val restResult = JsonUtil.writeValueAsBytes(RestResponse.ok(true));
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<byte[]>> any())).thenReturn(resp);

    }

    @Test
    public void proxy() throws Exception {
        proxy(false, false, false);
        proxy(true, false, false);
        proxy(true, true, false);
        proxy(true, true, true);
        proxy(true, false, true);
        val server = "127.0.0.1:7979";
        proxy(server, false, false, false);
        proxy(server, false, false, true);
        proxy(null, false, false, false);
        proxy(null, true, false, false);
        proxy(null, true, true, false);
        proxy(null, true, true, true);
        proxy(null, true, false, true);
    }

    private void proxy(boolean needCookie, boolean isRouted, boolean needRoute) throws Exception {
        Appender appender = Mockito.mock(Appender.class);
        try {
            Mockito.when(appender.getName()).thenReturn("mocked");
            Mockito.when(appender.isStarted()).thenReturn(true);
            ((Logger) LogManager.getRootLogger()).addAppender(appender);
            Mockito.when(routeService.needRoute()).thenReturn(needRoute);
            val req = new MockHttpServletRequest();
            if (needCookie) {
                val server = "127.0.0.1:7070";
                val serverBytes = server.getBytes(Charset.defaultCharset());
                val serverCookie = new String(encodeBase64(serverBytes), Charset.defaultCharset());
                Cookie cookie = new Cookie("server", serverCookie);
                cookie.setPath(SparderUIUtil.KYLIN_UI_BASE);
                req.setCookies(cookie);
            }
            req.addHeader("routed", isRouted);
            val res = new MockHttpServletResponse();
            sparderUIService.proxy(req, res);

            ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
            Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
            var logs = logCaptor.getAllValues().stream()
                    .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.SparderUIService"))
                    .filter(event -> event.getLevel().equals(Level.INFO))
                    .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
            if (needCookie && needRoute && !isRouted) {
                assertEquals(1, logs.size());
                assertTrue(StringUtils.equals(logs.get(0), "proxy sparder UI to server : [127.0.0.1:7070]"));
            } else {
                assertTrue(CollectionUtils.isEmpty(logs));
            }
        } finally {
            ((Logger) LogManager.getRootLogger()).removeAppender(appender);
        }
    }

    private void proxy(String server, boolean needCookie, boolean isRouted, boolean needRoute) throws Exception {
        Appender appender = Mockito.mock(Appender.class);
        try {
            Mockito.when(appender.getName()).thenReturn("mocked");
            Mockito.when(appender.isStarted()).thenReturn(true);
            ((Logger) LogManager.getRootLogger()).addAppender(appender);
            Mockito.when(routeService.needRoute()).thenReturn(needRoute);
            val req = new MockHttpServletRequest();
            if (needCookie) {
                val serverBytes = "127.0.0.1:7979".getBytes(Charset.defaultCharset());
                val serverCookie = new String(encodeBase64(serverBytes), Charset.defaultCharset());
                Cookie cookie = new Cookie("server", serverCookie);
                cookie.setPath(SparderUIUtil.KYLIN_UI_BASE);
                req.setCookies(cookie);
            }
            req.addHeader("routed", isRouted);
            val res = new MockHttpServletResponse();
            sparderUIService.proxy("123", "321", server, req, res);

            ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
            Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
            var logs = logCaptor.getAllValues().stream()
                    .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.SparderUIService"))
                    .filter(event -> event.getLevel().equals(Level.INFO))
                    .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
            if ((StringUtils.isNotBlank(server) || needCookie) && needRoute && !isRouted) {
                assertEquals(1, logs.size());
                assertTrue(StringUtils.equals(logs.get(0),
                        "proxy sparder UI to server : [127.0.0.1:7979] queryId : [321] Id : [123]"));
            } else {
                assertTrue(CollectionUtils.isEmpty(logs));
            }
        } finally {
            ((Logger) LogManager.getRootLogger()).removeAppender(appender);
        }
    }
}