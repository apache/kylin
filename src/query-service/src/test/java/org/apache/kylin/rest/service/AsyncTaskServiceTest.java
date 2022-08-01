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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.query.QueryHistoryRequest;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDaoTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ServiceTestBase.SpringConfig.class)
@WebAppConfiguration(value = "src/main/resources")
@ActiveProfiles({ "testing", "test" })
public class AsyncTaskServiceTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    @Autowired
    private QueryHistoryService queryHistoryService;

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @AfterClass
    public static void tearDownResource() {
        staticCleanupTestMetadata();
    }

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        RDBMSQueryHistoryDAO.getInstance().deleteAllQueryHistory();
        cleanupTestMetadata();
    }

    @Test
    public void testDownloadQueryHistoriesTimeout() throws Exception {
        // prepare query history to RDBMS
        RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        queryHistoryDAO.deleteAllQueryHistory();
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311572000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311632000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311692000L, 1L, true, PROJECT, false));
        QueryMetrics queryMetrics = RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311752000L, 1L, true, PROJECT,
                false);
        queryMetrics.setSql("select * from PRT LIMIT 500");
        queryMetrics.setQueryStatus("FAILED");
        queryMetrics.getQueryHistoryInfo()
                .setQueryMsg("From line 1, column 15 to line 1, column 17: Object 'PRT' not found while executing SQL: "
                        + "\"select * from PRT LIMIT 500\"");
        queryHistoryDAO.insert(queryMetrics);

        // prepare request and response
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0]);
                TimeUnit.SECONDS.sleep(1);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class));

        queryHistoryService.downloadQueryHistories(request, response, ZoneOffset.ofHours(8), 8, false);
        assertEquals(
                "\uFEFFStart Time,Duration,Query ID,SQL Statement,Answered by,Query Status,Query Node,Submitter,Query Message\n"
                        + "2020-01-29 23:29:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select * from PRT LIMIT 500\",CONSTANTS,FAILED,,ADMIN,\"From line 1, column 15 to line 1, column 17: Object 'PRT' not found while executing SQL: \"\"select * from PRT LIMIT 500\"\"\"\n"
                        + "2020-01-29 23:28:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:27:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",\"[Deleted Model,Deleted Model]\",SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:26:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",\"[Deleted Model,Deleted Model]\",SUCCEEDED,,ADMIN,\n",
                baos.toString(StandardCharsets.UTF_8.name()));

        overwriteSystemProp("kylin.query.query-history-download-timeout-seconds", "3s");
        try {
            queryHistoryService.downloadQueryHistories(request, response, ZoneOffset.ofHours(8), 8, false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof TimeoutException);
        }
    }
}
