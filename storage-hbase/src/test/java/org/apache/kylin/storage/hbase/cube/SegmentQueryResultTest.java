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

package org.apache.kylin.storage.hbase.cube;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.common.QueryContext.CubeSegmentStatistics;
import org.apache.kylin.storage.hbase.cube.v2.SegmentQueryResult;
import org.apache.kylin.storage.hbase.cube.v2.SegmentQueryResult.Builder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;

public class SegmentQueryResultTest {
    private static final Logger logger = LoggerFactory.getLogger(SegmentQueryResultTest.class);

    @Test
    public void buildTest() {
        int maxCacheResultSize = 10 * 1024;
        ExecutorService rpcExecutor = Executors.newFixedThreadPool(4);
        SegmentQueryResult.Builder builder = new Builder(8, maxCacheResultSize);
        mockSendRPCTasks(rpcExecutor, 4, builder, 1024);
        assertFalse(builder.isComplete());
        mockSendRPCTasks(rpcExecutor, 4, builder, 1024);
        assertTrue(builder.isComplete());

        builder = new Builder(8, maxCacheResultSize);
        mockSendRPCTasks(rpcExecutor, 8, builder, 1500);
        assertFalse(builder.isComplete());
    }

    @Test
    public void resultValidateTest() {
        long segmentBuildTime = System.currentTimeMillis() - 1000;
        int maxCacheResultSize = 10 * 1024;
        ExecutorService rpcExecutor = Executors.newFixedThreadPool(4);
        SegmentQueryResult.Builder builder = new Builder(8, maxCacheResultSize);
        mockSendRPCTasks(rpcExecutor, 8, builder, 1024);
        CubeSegmentStatistics statistics = new CubeSegmentStatistics();
        statistics.setWrapper("cube1", "20171001000000-20171010000000", 3, 7, 1);
        builder.setCubeSegmentStatistics(statistics);
        SegmentQueryResult segmentQueryResult = builder.build();

        CubeSegmentStatistics desStatistics = SerializationUtils.deserialize(segmentQueryResult
                .getCubeSegmentStatisticsBytes());
        assertEquals("cube1", desStatistics.getCubeName());
    }

    private void mockSendRPCTasks(ExecutorService rpcExecutor, int rpcNum, SegmentQueryResult.Builder builder,
                                  int resultSize) {
        List<Future> futures = Lists.newArrayList();
        for (int i = 0; i < rpcNum; i++) {
            Future future = rpcExecutor.submit(new MockRPCTask(resultSize, 10, builder));
            futures.add(future);
        }
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                logger.error("exception", e);
            }
        }
    }

    private static class MockRPCTask implements Runnable {
        private int resultSize;
        private long takeTime;
        private SegmentQueryResult.Builder builder;

        MockRPCTask(int resultSize, long takeTime, SegmentQueryResult.Builder builder) {
            this.resultSize = resultSize;
            this.takeTime = takeTime;
            this.builder = builder;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(takeTime);
            } catch (InterruptedException e) {
                logger.error("interrupt", e);
            }
            builder.putRegionResult(new byte[resultSize]);
        }
    }

}
