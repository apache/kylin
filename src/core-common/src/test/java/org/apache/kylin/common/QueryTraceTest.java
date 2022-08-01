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

package org.apache.kylin.common;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class QueryTraceTest {

    @Test
    public void test() throws InterruptedException {
        QueryTrace trace = new QueryTrace();
        trace.startSpan("span 1");
        Thread.sleep(100);
        trace.startSpan("span 2");
        Thread.sleep(100);
        trace.endLastSpan();
        Assert.assertEquals(2, trace.spans().size());
        Assert.assertTrue(trace.getLastSpan().isPresent());
        Assert.assertEquals("span 2", trace.getLastSpan().get().name);
        assertTimeEqual(100, trace.getLastSpan().get().duration);

        trace.amendLast("span 2", trace.getLastSpan().get().start + trace.getLastSpan().get().getDuration() + 1000);
        assertTimeEqual(1100, trace.getLastSpan().get().duration);

        trace.startSpan("span 3");
        long duration = trace.calculateDuration("span 3",
                trace.getLastSpan().get().start + trace.getLastSpan().get().getDuration() + 1000);
        assertTimeEqual(999, duration);
    }

    @Test
    public void testGroups() {
        QueryTrace trace = new QueryTrace();
        trace.startSpan(QueryTrace.GET_ACL_INFO);
        trace.startSpan(QueryTrace.SQL_TRANSFORMATION);
        trace.startSpan(QueryTrace.SQL_PARSE_AND_OPTIMIZE);
        trace.startSpan(QueryTrace.MODEL_MATCHING);
        trace.startSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB);
        trace.startSpan(QueryTrace.WAIT_FOR_EXECUTION);
        trace.startSpan(QueryTrace.EXECUTION);
        trace.startSpan(QueryTrace.FETCH_RESULT);
        trace.startSpan(QueryTrace.SQL_PUSHDOWN_TRANSFORMATION);
        trace.startSpan(QueryTrace.HIT_CACHE);
        trace.endLastSpan();

        for (QueryTrace.Span span : trace.spans()) {
            if (QueryTrace.PREPARATION.equals(span.getGroup()) || QueryTrace.JOB_EXECUTION.equals(span.getGroup())) {
                Assert.assertTrue(QueryTrace.SPAN_GROUPS.containsKey(span.getName()));
            } else {
                Assert.assertFalse(QueryTrace.SPAN_GROUPS.containsKey(span.getName()));
            }
        }
    }

    @Test
    public void testGroupsOfSPARK_JOB_EXECUTION() {
        QueryTrace trace = new QueryTrace();
        trace.startSpan(QueryTrace.GET_ACL_INFO);
        trace.startSpan(QueryTrace.SQL_TRANSFORMATION);
        trace.startSpan(QueryTrace.SQL_PARSE_AND_OPTIMIZE);
        trace.startSpan(QueryTrace.MODEL_MATCHING);
        trace.startSpan(QueryTrace.SPARK_JOB_EXECUTION);
        trace.startSpan(QueryTrace.SQL_PUSHDOWN_TRANSFORMATION);
        trace.startSpan(QueryTrace.HIT_CACHE);
        trace.endLastSpan();

        for (QueryTrace.Span span : trace.spans()) {
            if (QueryTrace.PREPARATION.equals(span.getGroup())) {
                Assert.assertTrue(QueryTrace.SPAN_GROUPS.containsKey(span.getName()));
            } else {
                Assert.assertFalse(QueryTrace.SPAN_GROUPS.containsKey(span.getName()));
            }
        }
    }

    private void assertTimeEqual(long expected, long actual) {
        Assert.assertTrue(Math.abs(expected - actual) < 1000);
    }

}
