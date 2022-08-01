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
package org.apache.kylin.metadata.epoch;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import lombok.val;

@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
public class EpochUpdateLockManagerTest {

    private final String project = "test";

    @Test
    public void testGetLock() {
        val executorService = Executors.newFixedThreadPool(5);

        try {
            val lockList = Lists.newCopyOnWriteArrayList();

            for (int i = 0; i < 10; i++) {
                executorService.submit(() -> {
                    lockList.add(EpochUpdateLockManager.getLock(project));
                });
            }
            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

            val lockCache = EpochUpdateLockManager.getLock(project);

            Assert.assertTrue(lockList.stream().allMatch(x -> lockCache == x));

            Assert.assertEquals(EpochUpdateLockManager.getInstance().getLockCacheSize(), 1);

        } catch (Exception e) {
            Assert.fail("test error," + Throwables.getRootCause(e).getMessage());
        }
    }

    @Test
    public void testGetLockBatch() {
        val executorService = Executors.newFixedThreadPool(2);

        try {
            val lockList = Lists.newCopyOnWriteArrayList();

            val projectList = Lists.<List<String>> newArrayList();
            projectList.add(Arrays.asList("p1", "p3", "p2", "p4"));
            projectList.add(Arrays.asList("p3", "p2", "p1", "p4"));

            for (List<String> locks : projectList) {
                executorService.submit(() -> {
                    lockList.add(EpochUpdateLockManager.getLock(locks));
                });
            }
            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

            Assert.assertTrue(ArrayUtils.isEquals(lockList.get(0), lockList.get(1)));

        } catch (Exception e) {
            Assert.fail("test error," + Throwables.getRootCause(e).getMessage());
        }
    }

    @Test
    public void testExecuteLockBatch() {
        val executorService = Executors.newFixedThreadPool(3);
        try {

            val lockList = Lists.<List<String>> newArrayList();
            lockList.add(Arrays.asList("p1", "p2", "p3"));
            lockList.add(Arrays.asList("p2", "p3", "p4"));
            lockList.add(Arrays.asList("p3", "p4", "p2", "p3"));

            val resultList = Lists.<String> newCopyOnWriteArrayList();
            for (List<String> locks : lockList) {
                executorService.submit(() -> {
                    EpochUpdateLockManager.executeEpochWithLock(locks, () -> {
                        val r = String.join(",", locks);
                        resultList.add(r);
                        return r;
                    });
                });
            }

            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

            Assert.assertEquals(resultList.size(), lockList.size());

        } catch (Exception e) {
            Assert.fail("test error," + Throwables.getRootCause(e).getMessage());
        }
    }
}
