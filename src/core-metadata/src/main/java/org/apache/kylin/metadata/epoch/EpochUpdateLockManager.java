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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.kylin.common.Singletons;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EpochUpdateLockManager {

    private final Map<String, ReentrantLock> epochLockCache;

    private EpochUpdateLockManager() {
        epochLockCache = Maps.newConcurrentMap();
    }

    public interface Callback<T> {
        T handle() throws Exception;
    }

    private Lock getLockFromCache(String project) throws ExecutionException {
        if (!epochLockCache.containsKey(project)) {
            synchronized (epochLockCache) {
                epochLockCache.putIfAbsent(project, new ReentrantLock(true));
            }
        }
        return epochLockCache.get(project);
    }

    @VisibleForTesting
    static EpochUpdateLockManager getInstance() {
        return Singletons.getInstance(EpochUpdateLockManager.class, clz -> {
            try {
                return new EpochUpdateLockManager();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @VisibleForTesting
    long getLockCacheSize() {
        return epochLockCache.size();
    }

    @VisibleForTesting
    static Lock getLock(String project) {
        try {
            return EpochUpdateLockManager.getInstance().getLockFromCache(project);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    static List<Lock> getLock(@Nonnull List<String> projects) {
        Preconditions.checkNotNull(projects, "projects is null");

        val sortedProjects = new ArrayList<>(projects);
        sortedProjects.sort(Comparator.naturalOrder());
        return sortedProjects.stream().map(EpochUpdateLockManager::getLock).collect(Collectors.toList());
    }

    public static <T> T executeEpochWithLock(@Nonnull String project, @Nonnull Callback<T> callback) {
        Preconditions.checkNotNull(project, "project is null");
        Preconditions.checkNotNull(callback, "callback is null");

        val lock = getLock(project);

        Preconditions.checkNotNull(lock, "lock from cache is null");

        try {
            lock.lock();
            return callback.handle();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * be carefully to avoid dead lock
     * @param projects you should
     * @param callback
     * @param <T>
     * @return
     */
    static <T> T executeEpochWithLock(@Nonnull List<String> projects, @Nonnull Callback<T> callback) {
        Preconditions.checkNotNull(projects, "projects is null");
        Preconditions.checkNotNull(callback, "callback is null");

        if (projects.size() == 1) {
            return executeEpochWithLock(projects.get(0), callback);
        }
        List<Lock> locks = getLock(projects);

        Preconditions.checkState(locks.stream().allMatch(Objects::nonNull), "locks from cache is null");

        try {
            locks.forEach(Lock::lock);
            return callback.handle();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            Collections.reverse(locks);
            locks.forEach(Lock::unlock);
        }
    }

}
