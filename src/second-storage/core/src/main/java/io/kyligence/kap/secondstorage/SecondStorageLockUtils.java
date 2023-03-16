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

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.SegmentRange;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;


public class SecondStorageLockUtils {
    private static final Map<Pair<String, SegmentRange<Long>>, Lock> JOB_LOCKS = new ConcurrentHashMap<>();
    private static final Object guard = new Object();

    public static boolean containsKey(String modelId) {
        return JOB_LOCKS.keySet().stream().anyMatch(item -> item.getFirst().equals(modelId));
    }
    public static boolean containsKey(String modelId, SegmentRange<Long> range) {
        return JOB_LOCKS.keySet().stream().anyMatch(item -> item.getFirst().equals(modelId) && item.getSecond().overlaps(range));
    }

    private static Optional<Pair<String, SegmentRange<Long>>> getOverlapKey(String modelId, SegmentRange<Long> range) {
        return JOB_LOCKS.keySet().stream().filter(item -> item.getFirst().equals(modelId) && item.getSecond().overlaps(range)).findFirst();
    }


    public static Lock acquireLock(String modelId, SegmentRange<Long> range) {
        Preconditions.checkNotNull(modelId);
        Preconditions.checkNotNull(range);
        int second = KylinConfig.getInstanceFromEnv().getSecondStorageWaitLockTimeout();
        synchronized (guard) {
            while (containsKey(modelId, range)) {
                Optional<Pair<String, SegmentRange<Long>>> key = getOverlapKey(modelId, range);
                if (key.isPresent()) {
                    Lock lock = JOB_LOCKS.get(key.get());
                    try {
                        if (lock.tryLock(second, TimeUnit.SECONDS)) {
                            lock.unlock();
                            JOB_LOCKS.remove(key.get());
                        } else {
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            if (containsKey(modelId, range)) {
                throw new IllegalStateException("Can't acquire job lock, job is running now.");
            }
            Preconditions.checkArgument(!containsKey(modelId, range));
            Lock lock = new ReentrantLock();
            JOB_LOCKS.put(new Pair<>(modelId, range), lock);
            return lock;
        }
    }

    public static void unlock(String modelId, SegmentRange<Long> range) {
        Pair<String, SegmentRange<Long>> key = new Pair<>(modelId, range);
        if (!JOB_LOCKS.containsKey(key)) {
            throw new IllegalStateException(String.format(Locale.ROOT,
                    "Logical Error! This is a bug. Lock for model %s:%s is lost", modelId, range));
        }
        JOB_LOCKS.get(key).unlock();
        JOB_LOCKS.remove(key);
    }

    private SecondStorageLockUtils() {
    }
}
