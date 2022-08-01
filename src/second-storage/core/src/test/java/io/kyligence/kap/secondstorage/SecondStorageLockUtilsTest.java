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

package io.kyligence.kap.secondstorage;

import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SecondStorageLockUtilsTest {

    @Test(expected = ExecutionException.class)
    public void acquireLock() throws Exception {
        String modelId = RandomUtil.randomUUIDStr();
        SegmentRange<Long> range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        SegmentRange<Long> range2 = new SegmentRange.TimePartitionedSegmentRange(1L, 2L);
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Future<Boolean> future = executorService.submit(() -> {
            SecondStorageLockUtils.acquireLock(modelId, range2).lock();
            return true;
        });
        future.get();
    }

    @Test
    public void testDoubleAcquireLock() throws Exception {
        String modelId = RandomUtil.randomUUIDStr();
        SegmentRange<Long> range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        Assert.assertTrue(SecondStorageLockUtils.containsKey(modelId, range));
    }

    @Test(expected = IllegalStateException.class)
    public void testUnlockFailed() {
        SecondStorageLockUtils.unlock(RandomUtil.randomUUIDStr(), SegmentRange.TimePartitionedSegmentRange.createInfinite());
    }
}