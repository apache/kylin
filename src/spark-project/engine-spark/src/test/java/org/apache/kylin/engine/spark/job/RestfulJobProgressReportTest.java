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
package org.apache.kylin.engine.spark.job;

import static org.apache.kylin.engine.spark.job.RestfulJobProgressReport.JOB_HAS_STOPPED;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

@MetadataInfo(onlyProps = true)
public class RestfulJobProgressReportTest {

    private static MockWebServer server;

    static final Dispatcher dispatcher = new Dispatcher() {

        @SneakyThrows
        @Override
        public MockResponse dispatch(RecordedRequest request) {
            HashMap<String, String> jobStoppedResponse = new HashMap<>();
            jobStoppedResponse.put("code", KylinException.CODE_UNDEFINED);
            jobStoppedResponse.put("data", "");
            jobStoppedResponse.put("msg", JOB_HAS_STOPPED);

            switch (request.getPath()) {
            case "/api/jobs/stage/status":
                return new MockResponse().setResponseCode(200)
                        .setBody(JsonUtil.writeValueAsIndentString(jobStoppedResponse));
            default:
                return new MockResponse().setResponseCode(404);
            }
        }
    };

    @BeforeAll
    static void setUp() throws IOException {
        int serverPort = RandomUtils.nextInt() % (65536 - 7070) + 7070;
        System.setProperty("spark.driver.rest.server.address", "127.0.0.1:" + serverPort);
        server = new MockWebServer();
        server.setDispatcher(dispatcher);
        server.start(serverPort);
    }

    @AfterAll
    static void destroy() throws IOException {
        server.close();
        System.clearProperty("spark.driver.rest.server.address");
    }

    @Test
    void updateSparkJobInfoTest() {
        RestfulJobProgressReport report = new RestfulJobProgressReport();
        try {
            HashMap<String, String> params = new HashMap<>();
            params.put(ParamsConstants.TIME_OUT, "3000");
            report.updateSparkJobInfo(params, "/api/jobs/stage/status", "");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalStateException);
            Assertions.assertTrue(e.getMessage().startsWith(JOB_HAS_STOPPED));
        }
    }

    @Test
    void updateSparkJobExtraInfo() {
        RestfulJobProgressReport report = new RestfulJobProgressReport();
        try {
            HashMap<String, String> params = new HashMap<>();
            params.put(ParamsConstants.TIME_OUT, "3000");
            report.updateSparkJobExtraInfo(params, "/api/jobs/stage/status", "default", "a", new HashMap<>());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalStateException);
            Assertions.assertTrue(e.getMessage().startsWith(JOB_HAS_STOPPED));
        }
    }
}
