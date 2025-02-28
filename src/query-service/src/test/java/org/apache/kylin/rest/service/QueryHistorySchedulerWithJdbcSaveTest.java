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

import static org.apache.kylin.metadata.query.QueryMetrics.QUERY_RESPONSE_TIME;
import static org.apache.kylin.metadata.query.RDBMSQueryHistoryDaoTest.createQueryMetrics;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import lombok.val;

public class QueryHistorySchedulerWithJdbcSaveTest extends NLocalFileMetadataTestCase {

    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Mock
    protected Appender appender = Mockito.mock(Appender.class);

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=");
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        Mockito.when(appender.getName()).thenReturn("mocked");
        Mockito.when(appender.isStarted()).thenReturn(true);
        ((Logger) LogManager.getRootLogger()).addAppender(appender);
    }

    @After
    public void destroy() throws Exception {
        ((Logger) LogManager.getRootLogger()).removeAppender(appender);
        queryHistoryDAO.deleteAllQueryHistory();
        cleanupTestMetadata();
    }

    @Test
    public void testInsertAndUpdate() throws Exception {
        QueryMetrics queryMetrics = createQueryMetrics(1584888338274L, 5578L, true, "default", true);

        QueryHistoryScheduler queryHistoryScheduler = QueryHistoryScheduler.getInstance();

        QueryMetrics queryMetricsUpdate = new QueryMetrics(queryMetrics.getQueryId());
        QueryHistoryInfo info = new QueryHistoryInfo();
        info.getTraces().add(new QueryHistoryInfo.QueryTraceSpan(QUERY_RESPONSE_TIME, null, 123));
        queryMetricsUpdate.setQueryHistoryInfo(info);
        queryMetricsUpdate.setUpdateMetrics(true);

        QueryMetrics queryMetricsUpdate2 = new QueryMetrics(queryMetrics.getQueryId());
        QueryHistoryInfo info2 = new QueryHistoryInfo();
        info2.getTraces().add(new QueryHistoryInfo.QueryTraceSpan(QUERY_RESPONSE_TIME, null, 321));
        queryMetricsUpdate2.setQueryHistoryInfo(info2);
        queryMetricsUpdate2.setUpdateMetrics(true);

        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetricsUpdate);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetricsUpdate2);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetricsUpdate);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        Assert.assertEquals(7, queryHistoryScheduler.queryMetricsQueue.size());

        // consume
        queryHistoryScheduler.init();

        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
            Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
            val logs = logCaptor.getAllValues().stream()
                    .filter(logEvent -> "org.apache.kylin.metadata.query.JdbcQueryHistoryStore"
                            .equals(logEvent.getLoggerName()))
                    .filter(event -> Level.INFO.equals(event.getLevel()))
                    .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
            return logs.stream().anyMatch(log -> StringUtils.contains(log, "Update 4 query history info takes"));
        });

        List<QueryHistory> queryHistories = RDBMSQueryHistoryDAO.getInstance()
                .getByQueryIds(Lists.newArrayList(queryMetrics.getQueryId()));
        Assert.assertEquals(4, queryHistories.size());
        for (QueryHistory queryHistory : queryHistories) {
            Assert.assertEquals(queryMetrics.getQueryId(), queryHistory.getQueryId());
            List<QueryHistoryInfo.QueryTraceSpan> traces = queryHistory.getQueryHistoryInfo().getTraces();
            Assert.assertTrue(traces.stream().anyMatch(span -> QUERY_RESPONSE_TIME.equals(span.getName())
                    && span.getDuration() == 321 && span.getGroup() == null));
        }
    }

}
