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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.query.util.QueryInfoCollector;
import org.apache.kylin.rest.request.SQLRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BadQueryDetectorTest extends LocalFileMetadataTestCase {

    @Before
    public void before() {
        super.createTestMetadata();
    }

    @After
    public void after() {
        super.cleanupTestMetadata();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test() throws InterruptedException {
        int alertMB = BadQueryDetector.getSystemAvailMB() * 2;
        int alertRunningSec = 5;
        String mockSql = "select * from just_a_test";
        final ArrayList<String[]> alerts = new ArrayList<>();

        BadQueryDetector badQueryDetector = new BadQueryDetector(alertRunningSec * 1000, alertMB, alertRunningSec, 1000);
        badQueryDetector.registerNotifier(new BadQueryDetector.Notifier() {
            @Override
            public void badQueryFound(String adj, float runningSec, long startTime, String project, String sql,
                    String user, Thread t, String queryId, QueryInfoCollector collect) {
                alerts.add(new String[] { adj, sql });
            }
        });
        badQueryDetector.start();

        {
            Thread.sleep(1000);

            SQLRequest sqlRequest = new SQLRequest();
            sqlRequest.setSql(mockSql);
            badQueryDetector.queryStart(Thread.currentThread(), sqlRequest, "user", RandomUtil.randomUUID().toString());

            // make sure bad query check happens twice
            Thread.sleep((alertRunningSec * 2 + 1) * 1000);

            badQueryDetector.queryEnd(Thread.currentThread(), BadQueryEntry.ADJ_PUSHDOWN);
        }

        badQueryDetector.interrupt();

        assertEquals(2, alerts.size());
        // second check founds a Slow
        assertArrayEquals(new String[] { BadQueryEntry.ADJ_SLOW, mockSql }, alerts.get(0));
        // end notifies a Pushdown
        assertArrayEquals(new String[] { BadQueryEntry.ADJ_PUSHDOWN, mockSql }, alerts.get(1));
    }

    @Test
    public void testSlowQuery() throws InterruptedException, IOException {
        int alertMB = BadQueryDetector.getSystemAvailMB() * 2;
        int alertRunningSec = 2;
        String mockSql = "select * from just_a_test";

        BadQueryDetector badQueryDetector = new BadQueryDetector(alertRunningSec * 1000, alertMB, alertRunningSec, 1000);
        badQueryDetector.start();

        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setProject("test_project");
        sqlRequest.setSql(mockSql);
        badQueryDetector.queryStart(Thread.currentThread(), sqlRequest, "user", RandomUtil.randomUUID().toString());

        try {
            Thread.sleep(1000);

            QueryInfoCollector.current().setCubeNames(Lists.newArrayList("[CUBE[name=TEST_CUBE]]"));

            Thread.sleep((alertRunningSec * 2 + 1) * 1000);

            BadQueryHistory badQueryHistory = BadQueryHistoryManager.getInstance(KylinConfig.getInstanceFromEnv()).getBadQueriesForProject("test_project");
            Set entries = badQueryHistory.getEntries();

            assertEquals(1, entries.size());

            BadQueryEntry entry = (BadQueryEntry) ((NavigableSet) entries).pollFirst();

            assertNotNull(entry);
            assertEquals(BadQueryEntry.ADJ_SLOW, entry.getAdj());
            assertEquals("[CUBE[name=TEST_CUBE]]", entry.getCube());
        } finally {
            badQueryDetector.queryEnd(Thread.currentThread(), BadQueryEntry.ADJ_SLOW);
            badQueryDetector.interrupt();
            QueryInfoCollector.reset();
        }
    }
}
