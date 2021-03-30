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

package org.apache.kylin.storage.hbase.cube.v2;

import org.apache.kylin.shaded.com.google.common.collect.Queues;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.common.QueryContext.CubeSegmentStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * query result for each segment
 */
public class SegmentQueryResult implements Serializable {
    private static final long serialVersionUID = 9047493994209284453L;

    private Collection<byte[]> regionResults;

    // store segment query stats for cube planer usage
    private byte[] cubeSegmentStatisticsBytes;

    public void setRegionResults(Collection<byte[]> regionResults) {
        this.regionResults = regionResults;
    }

    public Collection<byte[]> getRegionResults() {
        return regionResults;
    }

    public byte[] getCubeSegmentStatisticsBytes() {
        return cubeSegmentStatisticsBytes;
    }

    public void setCubeSegmentStatisticsBytes(byte[] cubeSegmentStatisticsBytes) {
        this.cubeSegmentStatisticsBytes = cubeSegmentStatisticsBytes;
    }

    public static class Builder {
        private static final Logger logger = LoggerFactory.getLogger(Builder.class);

        private volatile int regionsNum;
        private ConcurrentLinkedQueue<byte[]> queue;
        private AtomicInteger totalResultSize;
        private volatile int maxSegmentCacheSize;
        private byte[] cubeSegmentStatisticsBytes;

        public Builder(int regionsNum, int maxCacheResultSize) {
            this.regionsNum = regionsNum;
            this.queue = Queues.newConcurrentLinkedQueue();
            this.totalResultSize = new AtomicInteger();
            this.maxSegmentCacheSize = maxCacheResultSize;
        }

        public void putRegionResult(byte[] result) {
            totalResultSize.addAndGet(result.length);
            if (totalResultSize.get() > maxSegmentCacheSize) {
                logger.info("stop put result to cache, since the result size:{} is larger than configured size:{}",
                        totalResultSize.get(), maxSegmentCacheSize);
                return;
            }
            queue.offer(result);
        }

        public void setCubeSegmentStatistics(CubeSegmentStatistics cubeSegmentStatistics) {
            this.cubeSegmentStatisticsBytes = (cubeSegmentStatistics == null ? null : SerializationUtils
                    .serialize(cubeSegmentStatistics));
        }

        public boolean isComplete() {
            return queue.size() == regionsNum;
        }

        public SegmentQueryResult build() {
            SegmentQueryResult result = new SegmentQueryResult();
            result.setCubeSegmentStatisticsBytes(cubeSegmentStatisticsBytes);
            result.setRegionResults(queue);
            return result;
        }
    }
}
