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

import static org.apache.kylin.metadata.query.QueryMetrics.QUERY_RESPONSE_TIME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.rest.service.QueryHistoryScheduler;
import org.apache.spark.SparkEnv;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.util.ReflectionUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
@PrepareForTest({ QueryHistoryScheduler.class, KylinConfig.class })
public class QueryInterceptorTest {
    @Test
    public void queryBlockClean() throws Exception {
        QueryInterceptor interceptor = new QueryInterceptor();
        QueryContext.current().setExecutionID("1");
        SparkEnv mockEnv = Mockito.mock(SparkEnv.class);
        SparkEnv.set(mockEnv);
        interceptor.afterCompletion(null, null, null, null);
        Mockito.verify(mockEnv, Mockito.times(1)).deleteAllBlockForQueryResult("1");
        interceptor.afterCompletion(null, null, null, null);
        Mockito.verify(mockEnv, Mockito.times(1)).deleteAllBlockForQueryResult("1");
    }

    @Test
    public void queryResponseDuration() throws Exception {
        PowerMockito.mockStatic(QueryHistoryScheduler.class, KylinConfig.class);
        KylinConfig config = Mockito.mock(KylinConfig.class);
        PowerMockito.when(KylinConfig.getInstanceFromEnv()).thenAnswer(invocation -> config);
        Mockito.when(config.getQueryHistoryBufferSize()).thenReturn(500);
        QueryHistoryScheduler queryHistoryScheduler = Mockito.spy(QueryHistoryScheduler.class);
        PowerMockito.when(QueryHistoryScheduler.getInstance()).thenAnswer(invocation -> queryHistoryScheduler);

        Field field = ReflectionUtils.findField(QueryHistoryScheduler.class, "queryMetricsQueue");
        ReflectionUtils.makeAccessible(field);
        BlockingQueue<QueryMetrics> queryMetricsQueue = (BlockingQueue<QueryMetrics>) ReflectionUtils.getField(field,
                queryHistoryScheduler);

        QueryInterceptor interceptor = new QueryInterceptor();
        interceptor.afterCompletion(null, null, null, null);
        assertEquals(0, queryMetricsQueue.size());

        String queryId = RandomUtil.randomUUIDStr();
        QueryContext.current().setResponseStartTime(System.currentTimeMillis());
        QueryContext.current().setQueryId(queryId);
        interceptor.afterCompletion(null, null, null, null);
        assertEquals(1, queryMetricsQueue.size());

        QueryMetrics take = queryMetricsQueue.take();
        assertEquals(queryId, take.getQueryId());
        assertTrue(take.isUpdateMetrics());
        List<QueryHistoryInfo.QueryTraceSpan> traces = take.getQueryHistoryInfo().getTraces();
        assertEquals(1, traces.size());
        final QueryHistoryInfo.QueryTraceSpan queryTraceSpan = traces.get(0);
        assertEquals(QUERY_RESPONSE_TIME, queryTraceSpan.getName());
        assertNull(queryTraceSpan.getGroup());
        assertTrue(queryTraceSpan.getDuration() >= 0);
    }
}
